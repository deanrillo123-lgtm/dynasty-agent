import os
import json
import smtplib
import re
import hashlib
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from dateutil import tz
import pytz
import pandas as pd

import feedparser
import statsapi  # MLB-StatsAPI
from pybaseball import batting_stats, pitching_stats  # FanGraphs tables (MLB)

TZ_NAME = "America/Chicago"

SENDER = os.getenv("EMAIL_ADDRESS", "").strip()
SENDER_PW = os.getenv("EMAIL_PASSWORD", "").strip()
RECIPIENT = os.getenv("RECIPIENT_EMAIL", "").strip()

ROSTER_PATH = os.getenv("ROSTER_PATH", "roster.csv")

STATE_DIR = "state"
STATE_PATH = os.path.join(STATE_DIR, "state.json")

# Weekly logs
WEEKLY_OFFICIAL_PATH = os.path.join(STATE_DIR, "weekly_official.jsonl")  # transactions wire
WEEKLY_REPORTS_PATH = os.path.join(STATE_DIR, "weekly_reports.jsonl")    # MLB.com + MLBTR RSS matches

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SPORT_ID_MLB = 1
SPORT_ID_MILB = 21

RSS_SOURCES = [
    {"name": "MLB.com", "url": "https://www.mlb.com/feeds/news/rss.xml"},
    {"name": "MLB Trade Rumors", "url": "https://www.mlbtraderumors.com/feed"},
]


# ----------------------------
# Helpers / State
# ----------------------------
def local_now():
    return datetime.now(pytz.timezone(TZ_NAME))


def now_utc():
    return datetime.now(tz=tz.tzutc())


def ensure_state_files():
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(STATE_PATH):
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_run_utc": None,          # used for official transactions
                    "player_cache": {},            # name -> mlbam_id
                    "seen_rss_ids": [],            # dedupe RSS items
                    "last_daily_local_date": None  # prevents double-send when cron runs twice (DST-safe)
                },
                f,
                indent=2,
            )

    for p in [WEEKLY_OFFICIAL_PATH, WEEKLY_REPORTS_PATH]:
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                f.write("")


def load_state():
    ensure_state_files()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def append_jsonl(path: str, record: dict):
    ensure_state_files()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: str) -> list[dict]:
    ensure_state_files()
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    out.sort(key=lambda x: x.get("utc", ""))
    return out


def reset_jsonl(path: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write("")


# ----------------------------
# Email
# ----------------------------
def send_email(subject: str, body: str):
    if not (SENDER and SENDER_PW and RECIPIENT):
        raise RuntimeError("Missing EMAIL_ADDRESS / EMAIL_PASSWORD / RECIPIENT_EMAIL secrets.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = RECIPIENT
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER, SENDER_PW)
        server.send_message(msg)


# ----------------------------
# Roster
# ----------------------------
def load_roster() -> pd.DataFrame:
    # Fantrax exports can have inconsistent columns/quotes -> tolerate parsing.
    try:
        df = pd.read_csv(ROSTER_PATH, engine="python")
    except Exception:
        df = pd.read_csv(ROSTER_PATH, engine="python", on_bad_lines="skip")

    # Try common name columns; fallback to first column
    name_col = None
    for c in df.columns:
        if c.lower() in ["player", "player name", "name"]:
            name_col = c
            break
    if name_col is None:
        name_col = df.columns[0]

    out = df.copy()
    out["player_name"] = out[name_col].astype(str).str.strip()
    out = out[out["player_name"].ne("")]
    out = out[~out["player_name"].str.lower().isin(["player", "player name", "name"])]
    return out[["player_name"]].drop_duplicates()


def lookup_mlbam_id(player_name: str, state) -> int | None:
    cache = state.get("player_cache", {})
    if player_name in cache and "mlbam_id" in cache[player_name]:
        return cache[player_name]["mlbam_id"]

    try:
        res = statsapi.lookup_player(player_name)
        if not res:
            return None
        mlbam_id = int(res[0]["id"])
        cache.setdefault(player_name, {})["mlbam_id"] = mlbam_id
        state["player_cache"] = cache
        return mlbam_id
    except Exception:
        return None


# ----------------------------
# OFFICIAL: Transactions wire
# ----------------------------
def fetch_transactions(mlbam_id: int) -> list[dict]:
    """
    Official transaction feed per player (hydrate=transactions).
    This is where many promotions/demotions/IL/assignments show up when MLB logs them.
    """
    try:
        data = statsapi.get("person", {"personId": mlbam_id, "hydrate": "transactions"})
        people = data.get("people", [])
        if not people:
            return []
        tx = people[0].get("transactions", [])
        return tx if isinstance(tx, list) else []
    except Exception:
        return []


def tx_since(tx_list: list[dict], since_utc: datetime) -> list[dict]:
    items = []
    for t in tx_list:
        tdate = t.get("date")
        if not tdate:
            continue
        try:
            t_dt = datetime.strptime(tdate, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        except Exception:
            continue
        if t_dt <= since_utc:
            continue

        desc = t.get("description") or t.get("typeDesc") or "Transaction update"
        items.append({"utc": t_dt.isoformat(), "desc": desc, "type": t.get("typeCode") or t.get("type") or ""})
    return items


# ----------------------------
# REPORTS / QUOTES: RSS layer
# ----------------------------
def _normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _content_id(source: str, title: str, link: str) -> str:
    raw = f"{source}|{title}|{link}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def _build_name_patterns(roster_names: list[str]) -> list[tuple[str, re.Pattern]]:
    patterns = []
    for full in roster_names:
        full = full.strip()
        if not full:
            continue
        parts = full.split()
        last = parts[-1] if parts else full
        # Full name exact word boundary
        patterns.append((full, re.compile(rf"\b{re.escape(full)}\b", re.IGNORECASE)))
        # Last name (only if long enough to reduce false positives)
        if len(last) >= 5:
            patterns.append((full, re.compile(rf"\b{re.escape(last)}\b", re.IGNORECASE)))
    return patterns


def fetch_reports_for_roster(roster_names: list[str], state: dict, max_items_per_source: int = 40) -> list[dict]:
    seen = state.get("seen_rss_ids", [])
    seen_set = set(seen)

    patterns = _build_name_patterns(roster_names)

    matched = []
    for src in RSS_SOURCES:
        feed = feedparser.parse(src["url"])
        entries = feed.entries[:max_items_per_source]

        for e in entries:
            title = _normalize_text(getattr(e, "title", ""))
            link = _normalize_text(getattr(e, "link", "")) or _normalize_text(getattr(e, "id", ""))
            summary = _normalize_text(getattr(e, "summary", "")) if hasattr(e, "summary") else ""
            blob = f"{title} {summary}"

            cid = _content_id(src["name"], title, link)
            if cid in seen_set:
                continue

            pub_dt = None
            if hasattr(e, "published_parsed") and e.published_parsed:
                try:
                    pub_dt = datetime(*e.published_parsed[:6]).replace(tzinfo=tz.tzutc())
                except Exception:
                    pub_dt = None
            if pub_dt is None:
                pub_dt = now_utc()

            players = set()
            for full_name, pat in patterns:
                if pat.search(blob):
                    players.add(full_name)

            if not players:
                continue

            for p in sorted(players):
                matched.append(
                    {
                        "utc": pub_dt.isoformat(),
                        "player": p,
                        "source": src["name"],
                        "title": title,
                        "link": link,
                        "cid": cid,
                    }
                )

            # mark as seen once
            seen_set.add(cid)

    # bound dedupe list
    state["seen_rss_ids"] = list(seen_set)[-2500:]
    return matched


# ----------------------------
# STATS (ALL PLAYERS) for Weekly email
# ----------------------------
def _safe_div(n, d):
    try:
        if d in (0, 0.0, None):
            return None
        return n / d
    except Exception:
        return None


def _pct(n, d):
    v = _safe_div(n, d)
    if v is None:
        return None
    return round(v * 100.0, 1)


def _level_from_split(split: dict, fallback: str = "") -> str:
    sport = split.get("sport") or {}
    league = split.get("league") or {}
    team = split.get("team") or {}

    for key in ["abbreviation", "name"]:
        if sport.get(key):
            return str(sport.get(key))
    for key in ["abbreviation", "name"]:
        if league.get(key):
            return str(league.get(key))
    if team.get("name"):
        return str(team.get("name"))
    return fallback


def fetch_stats_splits(
    person_ids: list[int],
    group: str,
    stats_kind: str,
    sport_id: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    params = {
        "group": group,
        "stats": stats_kind,
        "sportId": sport_id,
        "personIds": ",".join(str(x) for x in person_ids),
    }
    if start_date and end_date:
        params["startDate"] = start_date
        params["endDate"] = end_date

    try:
        data = statsapi.get("stats", params)
        stats_arr = data.get("stats", [])
        if not stats_arr:
            return []
        splits = stats_arr[0].get("splits", [])
        return splits if isinstance(splits, list) else []
    except Exception:
        return []


def stats_tables_all_players(
    roster_names: list[str],
    name_to_id: dict[str, int],
    year: int,
    weekly_start: date,
    weekly_end: date,
):
    ids = [name_to_id[n] for n in roster_names if n in name_to_id]

    ws = weekly_start.strftime("%Y-%m-%d")
    we = weekly_end.strftime("%Y-%m-%d")

    def get_group_tables(group: str):
        w_mlb = fetch_stats_splits(ids, group, "byDateRange", SPORT_ID_MLB, ws, we)
        s_mlb = fetch_stats_splits(ids, group, "season", SPORT_ID_MLB)
        w_milb = fetch_stats_splits(ids, group, "byDateRange", SPORT_ID_MILB, ws, we)
        s_milb = fetch_stats_splits(ids, group, "season", SPORT_ID_MILB)
        return (w_mlb, s_mlb, w_milb, s_milb)

    def splits_to_df(splits: list[dict], group: str) -> pd.DataFrame:
        rows = []
        for sp in splits:
            player = (sp.get("player") or {}).get("fullName") or (sp.get("player") or {}).get("name")
            if not player:
                continue
            stat = sp.get("stat") or {}
            lvl = _level_from_split(sp)

            if group == "hitting":
                pa = stat.get("plateAppearances")
                so = stat.get("strikeOuts")
                bb = stat.get("baseOnBalls")
                rows.append(
                    {
                        "Name": player,
                        "Level": lvl,
                        "G": stat.get("gamesPlayed"),
                        "H": stat.get("hits"),
                        "HR": stat.get("homeRuns"),
                        "RBI": stat.get("rbi"),
                        "SB": stat.get("stolenBases"),
                        "AVG": stat.get("avg"),
                        "OBP": stat.get("obp"),
                        "wRC+": None,
                        "K%": _pct(so, pa) if pa is not None else None,
                        "BB%": _pct(bb, pa) if pa is not None else None,
                    }
                )
            else:
                bf = stat.get("battersFaced")
                so = stat.get("strikeOuts")
                bb = stat.get("baseOnBalls")
                rows.append(
                    {
                        "Name": player,
                        "Level": lvl,
                        "GS": stat.get("gamesStarted"),
                        "IP": stat.get("inningsPitched"),
                        "ERA": stat.get("era"),
                        "FIP": None,
                        "K%": _pct(so, bf) if bf is not None else None,
                        "BB%": _pct(bb, bf) if bf is not None else None,
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["Name"])
        df = pd.DataFrame(rows)

        # Collapse multi-splits to the most "substantial" line
        if group == "hitting" and "G" in df.columns:
            df["G_num"] = pd.to_numeric(df["G"], errors="coerce").fillna(0)
            df = df.sort_values(["Name", "G_num"], ascending=[True, False]).drop_duplicates("Name", keep="first")
            df = df.drop(columns=["G_num"], errors="ignore")
        if group == "pitching" and "IP" in df.columns:
            df["IP_num"] = pd.to_numeric(df["IP"], errors="coerce").fillna(0)
            df = df.sort_values(["Name", "IP_num"], ascending=[True, False]).drop_duplicates("Name", keep="first")
            df = df.drop(columns=["IP_num"], errors="ignore")

        return df

    # Hitters
    hw_mlb, hs_mlb, hw_milb, hs_milb = get_group_tables("hitting")
    hitters_week_mlb = splits_to_df(hw_mlb, "hitting")
    hitters_week_milb = splits_to_df(hw_milb, "hitting")
    hitters_season_mlb = splits_to_df(hs_mlb, "hitting")
    hitters_season_milb = splits_to_df(hs_milb, "hitting")

    # Pitchers
    pw_mlb, ps_mlb, pw_milb, ps_milb = get_group_tables("pitching")
    pitchers_week_mlb = splits_to_df(pw_mlb, "pitching")
    pitchers_week_milb = splits_to_df(pw_milb, "pitching")
    pitchers_season_mlb = splits_to_df(ps_mlb, "pitching")
    pitchers_season_milb = splits_to_df(ps_milb, "pitching")

    def merge_fill(primary: pd.DataFrame, fallback: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        if primary is None or primary.empty:
            base = fallback.copy()
        else:
            base = primary.copy()
            if fallback is not None and not fallback.empty:
                missing = set(fallback["Name"]) - set(base["Name"])
                if missing:
                    base = pd.concat([base, fallback[fallback["Name"].isin(missing)]], ignore_index=True)

        # Ensure all roster names appear
        all_rows = pd.DataFrame({"Name": roster_names})
        base = all_rows.merge(base, on="Name", how="left")

        for c in cols:
            if c not in base.columns:
                base[c] = None
        return base

    hitter_cols = ["Level", "G", "H", "HR", "RBI", "SB", "AVG", "OBP", "wRC+", "K%", "BB%"]
    pitcher_cols = ["Level", "GS", "IP", "ERA", "FIP", "K%", "BB%"]

    hitters_week = merge_fill(hitters_week_mlb, hitters_week_milb, hitter_cols)
    hitters_season = merge_fill(hitters_season_mlb, hitters_season_milb, hitter_cols)
    pitchers_week = merge_fill(pitchers_week_mlb, pitchers_week_milb, pitcher_cols)
    pitchers_season = merge_fill(pitchers_season_mlb, pitchers_season_milb, pitcher_cols)

    # Overlay FanGraphs advanced season stats where available
    try:
        fg_bat = batting_stats(year)
        fg_pit = pitching_stats(year)
        if "wRC+" in fg_bat.columns:
            hitters_season = hitters_season.merge(fg_bat[["Name", "wRC+"]], on="Name", how="left", suffixes=("", "_fg"))
            if "wRC+_fg" in hitters_season.columns:
                hitters_season["wRC+"] = hitters_season["wRC+_fg"].combine_first(hitters_season["wRC+"])
                hitters_season = hitters_season.drop(columns=["wRC+_fg"])
        if "FIP" in fg_pit.columns:
            pitchers_season = pitchers_season.merge(fg_pit[["Name", "FIP"]], on="Name", how="left", suffixes=("", "_fg"))
            if "FIP_fg" in pitchers_season.columns:
                pitchers_season["FIP"] = pitchers_season["FIP_fg"].combine_first(pitchers_season["FIP"])
                pitchers_season = pitchers_season.drop(columns=["FIP_fg"])
    except Exception as e:
        print(f"[stats] FanGraphs overlay failed: {e}")

    hitters_week = hitters_week[["Name"] + hitter_cols].sort_values("Name")
    hitters_season = hitters_season[["Name"] + hitter_cols].sort_values("Name")
    pitchers_week = pitchers_week[["Name"] + pitcher_cols].sort_values("Name")
    pitchers_season = pitchers_season[["Name"] + pitcher_cols].sort_values("Name")

    return hitters_week, hitters_season, pitchers_week, pitchers_season


# ----------------------------
# Daily + Weekly logic
# ----------------------------
def is_daily_send_time(state: dict) -> bool:
    """
    Daily send at ~6 AM Central. We run cron twice (DST-safe),
    so we gate here and also prevent duplicates by date.
    """
    ln = local_now()
    if ln.hour != 6:
        return False

    today_str = ln.strftime("%Y-%m-%d")
    if state.get("last_daily_local_date") == today_str:
        return False

    return True


def mark_daily_sent(state: dict):
    ln = local_now()
    state["last_daily_local_date"] = ln.strftime("%Y-%m-%d")


def run_daily(lookback_hours: int | None = None, force_send_if_any: bool = True):
    state = load_state()
    roster = load_roster()
    roster_names = roster["player_name"].tolist()

    # Gate scheduled daily sends to 6am CT
    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not is_daily_send_time(state):
            print("[daily] scheduled run but not 6am CT (or already sent). Skipping.")
            save_state(state)
            return

    # Determine "since" window
    if lookback_hours is not None:
        since = now_utc() - timedelta(hours=lookback_hours)
    else:
        last_run = state.get("last_run_utc")
        if last_run:
            since = datetime.fromisoformat(last_run)
            if since.tzinfo is None:
                since = since.replace(tzinfo=tz.tzutc())
        else:
            since = now_utc() - timedelta(hours=24)

    # OFFICIAL transactions
    official_found = []
    for player in roster_names:
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        items = tx_since(fetch_transactions(pid), since)
        for it in items:
            append_jsonl(WEEKLY_OFFICIAL_PATH, {"player": player, **it})
            official_found.append((player, it["desc"], it["utc"]))

    # REPORTS / QUOTES
    reports = fetch_reports_for_roster(roster_names, state)
    for r in reports:
        append_jsonl(
            WEEKLY_REPORTS_PATH,
            {
                "utc": r["utc"],
                "player": r["player"],
                "source": r["source"],
                "title": r["title"],
                "link": r["link"],
                "cid": r["cid"],
            },
        )

    # Update transaction since-window pointer
    state["last_run_utc"] = now_utc().isoformat()

    print(
        f"[daily] since={since.isoformat()} official_items={len(official_found)} "
        f"reports_items={len(reports)} roster={len(roster_names)}"
    )

    # Quiet mode: if no official and no reports, do nothing
    if not official_found and not reports:
        save_state(state)
        # still mark daily as sent only if scheduled, so you don't double-check forever
        if os.getenv("IS_SCHEDULED", "0") == "1":
            mark_daily_sent(state)
            save_state(state)
        return

    # Build email
    body = []
    body.append(f"# Dynasty Daily Update ({local_now().strftime('%a %b %d')})")
    body.append("")

    body.append("## Transaction Wire (Official)")
    if official_found:
        for player, desc, utc in sorted(official_found, key=lambda x: x[2]):
            body.append(f"- **{player}** — {desc}")
    else:
        body.append("No official transactions since last check.")
    body.append("")

    body.append("## Reports / Quotes (MLB.com + MLBTR)")
    if reports:
        reports_sorted = sorted(reports, key=lambda x: (x["player"], x["utc"]))
        cur = None
        for r in reports_sorted:
            if r["player"] != cur:
                cur = r["player"]
                body.append(f"- **{cur}**")
            body.append(f"  - ({r['source']}) {r['title']} — {r['link']}")
    else:
        body.append("No matched MLB.com / MLBTR items since last check.")
    body.append("")

    send_email(
        subject=f"Dynasty Daily Update — {local_now().strftime('%b %d')}",
        body="\n".join(body),
    )

    # Prevent double-send for the scheduled daily run
    if os.getenv("IS_SCHEDULED", "0") == "1":
        mark_daily_sent(state)

    save_state(state)


def should_send_weekly_now() -> bool:
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7  # Monday 7am CT


def build_weekly_email_body() -> str:
    roster = load_roster()
    roster_names = roster["player_name"].tolist()

    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = read_jsonl(WEEKLY_REPORTS_PATH)

    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (this_mon - timedelta(days=7)).date()
    end = (this_mon - timedelta(days=1)).date()

    body = []
    body.append(f"# Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}")
    body.append(f"_Weekly window: {start.strftime('%b %d')}–{end.strftime('%b %d')}_")
    body.append("")

    body.append("## Weekly News Recap — Transaction Wire (Official)")
    if not official_news:
        body.append("No official transactions logged this week.")
    else:
        for it in official_news:
            body.append(f"- **{it.get('player','')}** — {it.get('desc','')}")
    body.append("")

    body.append("## Weekly News Recap — Reports / Quotes (MLB.com + MLBTR)")
    if not reports_news:
        body.append("No matched MLB.com / MLBTR items logged this week.")
    else:
        reports_news = sorted(reports_news, key=lambda x: (x.get("player", ""), x.get("utc", "")))
        cur = None
        for it in reports_news:
            p = it.get("player", "")
            if p != cur:
                cur = p
                body.append(f"- **{cur}**")
            body.append(f"  - ({it.get('source','')}) {it.get('title','')} — {it.get('link','')}")
    body.append("")

    body.append("## Weekly Stats (All Players)")
    body.append("_wRC+ / FIP will be blank for players not available in FanGraphs MLB tables._")
    body.append("")

    state = load_state()
    name_to_id = {}
    for nm in roster_names:
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid
    save_state(state)

    year = now_local.year

    try:
        hitters_week, hitters_season, pitchers_week, pitchers_season = stats_tables_all_players(
            roster_names=roster_names,
            name_to_id=name_to_id,
            year=year,
            weekly_start=start,
            weekly_end=end,
        )

        body.append("### Hitters (Weekly)")
        body.append(hitters_week.to_markdown(index=False))
        body.append("")
        body.append("### Pitchers (Weekly)")
        body.append(pitchers_week.to_markdown(index=False))
        body.append("")
        body.append("## Season-to-date Stats (All Players)")
        body.append("")
        body.append("### Hitters (Season)")
        body.append(hitters_season.to_markdown(index=False))
        body.append("")
        body.append("### Pitchers (Season)")
        body.append(pitchers_season.to_markdown(index=False))
        body.append("")
    except Exception as e:
        body.append(f"Stats error: {e}")

    return "\n".join(body)


def run_weekly():
    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not should_send_weekly_now():
            print("[weekly] scheduled run but not Monday 7am CT. Skipping.")
            return

    body = build_weekly_email_body()
    send_email(subject=f"Dynasty Weekly Report — {local_now().strftime('%b %d, %Y')}", body=body)

    reset_jsonl(WEEKLY_OFFICIAL_PATH)
    reset_jsonl(WEEKLY_REPORTS_PATH)


# ----------------------------
# Test modes
# ----------------------------
def run_smtp_test():
    send_email(
        subject=f"Dynasty Agent SMTP Test — {local_now().strftime('%b %d %I:%M %p %Z')}",
        body="If you received this, GitHub Actions + Gmail SMTP secrets are working.",
    )


def run_news_test():
    send_email(
        subject="Dynasty Daily Update — TEST",
        body=(
            "# Dynasty Daily Update (TEST)\n\n"
            "## Transaction Wire (Official)\n"
            "- **Test Player** — Placed on IL (TEST)\n\n"
            "## Reports / Quotes (MLB.com + MLBTR)\n"
            "- **Test Prospect**\n"
            "  - (MLB.com) Prospect working at new position this spring — https://www.mlb.com/\n"
            "  - (MLB Trade Rumors) Team discussing roster role — https://www.mlbtraderumors.com/\n"
        ),
    )


def run_daily_realnews_test():
    """
    Real-news test: look back 14 days to maximize chance of finding items today,
    but still stays quiet if absolutely nothing matches your roster.
    """
    run_daily(lookback_hours=24 * 14)


def run_weekly_test():
    body = build_weekly_email_body()
    send_email(subject="Dynasty Weekly Report — TEST", body=body)


def main():
    ensure_state_files()
    mode = os.getenv("RUN_MODE", "daily").strip().lower()
    print(f"[main] RUN_MODE={mode}")

    if mode == "smtp_test":
        run_smtp_test()
    elif mode == "news_test":
        run_news_test()
    elif mode == "daily_realnews_test":
        run_daily_realnews_test()
    elif mode == "weekly_test":
        run_weekly_test()
    elif mode == "daily":
        run_daily()
    elif mode == "weekly":
        run_weekly()
    else:
        raise SystemExit("RUN_MODE must be one of: daily, weekly, smtp_test, news_test, daily_realnews_test, weekly_test")


if __name__ == "__main__":
    main()
