import os
import json
import smtplib
import re
import hashlib
import csv
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from dateutil import tz
import pytz
import pandas as pd

import feedparser
import statsapi
from pybaseball import batting_stats, pitching_stats

TZ_NAME = "America/Chicago"

SENDER = os.getenv("EMAIL_ADDRESS", "").strip()
SENDER_PW = os.getenv("EMAIL_PASSWORD", "").strip()
RECIPIENT = os.getenv("RECIPIENT_EMAIL", "").strip()

ROSTER_PATH = os.getenv("ROSTER_PATH", "roster.csv")

STATE_DIR = "state"
STATE_PATH = os.path.join(STATE_DIR, "state.json")

WEEKLY_OFFICIAL_PATH = os.path.join(STATE_DIR, "weekly_official.jsonl")
WEEKLY_REPORTS_PATH = os.path.join(STATE_DIR, "weekly_reports.jsonl")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SPORT_ID_MLB = 1
SPORT_ID_MILB = 21

# RSS Sources
MLB_NEWS_FEED = "https://www.mlb.com/feeds/news/rss.xml"
MLBTR_MAIN_FEED = "http://feeds.feedburner.com/MlbTradeRumors"
MLBTR_TX_FEED = "http://feeds.feedburner.com/MLBTRTransactions"
CBS_MLB_RSS = "https://www.cbssports.com/rss/headlines/mlb/"


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
                    "last_run_utc": None,
                    "player_cache": {},
                    "seen_rss_ids": [],
                    "last_daily_local_date": None,
                    "last_weekly_local_date": None,
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


def append_jsonl(path, record):
    ensure_state_files()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path):
    ensure_state_files()
    if not os.path.exists(path):
        return []
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
    return sorted(out, key=lambda x: x.get("utc", ""))


def reset_jsonl(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("")


# ----------------------------
# Email (HTML + plain fallback)
# ----------------------------
def send_email(subject: str, text_body: str, html_body: str):
    if not (SENDER and SENDER_PW and RECIPIENT):
        raise RuntimeError("Missing EMAIL_ADDRESS / EMAIL_PASSWORD / RECIPIENT_EMAIL secrets.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER
    msg["To"] = RECIPIENT

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER, SENDER_PW)
        server.send_message(msg)


# ----------------------------
# HTML helpers
# ----------------------------
def h(s: str) -> str:
    """HTML escape"""
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def news_button(url: str, label: str = "News") -> str:
    u = h(url)
    return (
        f"<a href='{u}' target='_blank' rel='noopener noreferrer' "
        "style='display:inline-block; padding:7px 10px; border-radius:8px; "
        "background:#1a73e8; color:#fff; text-decoration:none; font-size:13px;'>"
        f"{h(label)}</a>"
    )


def render_table_html(df: pd.DataFrame, title: str) -> str:
    if df is None or df.empty:
        return f"<h4 style='margin:14px 0 6px 0;'>{h(title)}</h4><div style='color:#666;'>No data.</div>"

    # Limit width/overflow a bit
    cols = list(df.columns)
    rows = df.fillna("").astype(str).values.tolist()

    out = []
    out.append(f"<h4 style='margin:16px 0 8px 0;'>{h(title)}</h4>")
    out.append(
        "<div style='overflow-x:auto; border:1px solid #e8e8e8; border-radius:10px;'>"
        "<table style='border-collapse:collapse; width:100%; font-size:12.5px;'>"
    )
    # header
    out.append("<thead>")
    out.append("<tr style='background:#f6f7f9;'>")
    for c in cols:
        out.append(
            "<th style='text-align:left; padding:8px 10px; border-bottom:1px solid #e8e8e8; "
            f"white-space:nowrap;'>{h(str(c))}</th>"
        )
    out.append("</tr>")
    out.append("</thead>")

    # body
    out.append("<tbody>")
    for i, r in enumerate(rows):
        bg = "#ffffff" if i % 2 == 0 else "#fbfbfc"
        out.append(f"<tr style='background:{bg};'>")
        for cell in r:
            out.append(
                "<td style='padding:7px 10px; border-bottom:1px solid #f0f0f0; white-space:nowrap;'>"
                f"{h(cell)}</td>"
            )
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


# ----------------------------
# Roster parsing (Fantrax format)
# ----------------------------
def load_roster():
    players = []
    teams = []

    idx_player = None
    idx_team = None

    with open(ROSTER_PATH, encoding="utf-8", errors="ignore") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row:
                continue

            # Header row for a section
            if "Player" in row and "Team" in row:
                idx_player = row.index("Player")
                idx_team = row.index("Team")
                continue

            if idx_player is None:
                continue

            if len(row) <= idx_player:
                continue

            name = row[idx_player].strip()
            if not name or name.lower() == "player":
                continue

            team = row[idx_team].strip() if idx_team is not None and len(row) > idx_team else ""
            name = re.sub(r"\s*\(.*\)$", "", name).strip()

            players.append(name)
            teams.append(team)

    df = pd.DataFrame({"player_name": players, "team_abbrev": teams})
    return df.drop_duplicates(subset=["player_name"]).reset_index(drop=True)


# ----------------------------
# MLBAM lookup
# ----------------------------
def lookup_mlbam_id(player_name, state):
    cache = state.get("player_cache", {})
    if player_name in cache:
        return cache[player_name]

    try:
        res = statsapi.lookup_player(player_name)
        if not res:
            return None
        pid = int(res[0]["id"])
        cache[player_name] = pid
        state["player_cache"] = cache
        return pid
    except Exception:
        return None


# ----------------------------
# Official transactions
# ----------------------------
def fetch_transactions(pid):
    try:
        data = statsapi.get("person", {"personId": pid, "hydrate": "transactions"})
        people = data.get("people", [])
        if not people:
            return []
        return people[0].get("transactions", [])
    except Exception:
        return []


def tx_since(tx_list, since):
    out = []
    for t in tx_list:
        d = t.get("date")
        if not d:
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        except Exception:
            continue
        if dt <= since:
            continue
        desc = t.get("description") or t.get("typeDesc") or "Transaction update"
        out.append({"utc": dt.isoformat(), "desc": desc})
    return out


# ----------------------------
# RSS / Google News
# ----------------------------
def _normalize(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def _content_id(source, title, link):
    return hashlib.sha1(f"{source}|{title}|{link}".encode("utf-8", errors="ignore")).hexdigest()


def _build_name_patterns(roster_names):
    # full-name only
    return [(name, re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)) for name in roster_names]


def _google_news_url(query):
    from urllib.parse import quote_plus
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


def _build_google_news_sources(roster_names):
    sources = []
    chunk_size = 8
    for i in range(0, len(roster_names), chunk_size):
        chunk = roster_names[i : i + chunk_size]
        or_part = " OR ".join([f"\"{n}\"" for n in chunk])
        query = f"({or_part}) baseball (injury OR soreness OR IL OR optioned OR promoted OR demoted OR trade OR traded OR DFA OR rehab)"
        sources.append({"name": f"Google News (Roster {i//chunk_size+1})", "url": _google_news_url(query)})

    sources.append({"name": "Google News (CBS Fantasy)", "url": _google_news_url("site:cbssports.com/fantasy baseball")})
    sources.append({"name": "Google News (RotoBaller)", "url": _google_news_url("site:rotoballer.com baseball")})

    return sources


def fetch_reports(roster_names, state):
    sources = [
        {"name": "MLB.com", "url": MLB_NEWS_FEED},
        {"name": "MLBTR Main", "url": MLBTR_MAIN_FEED},
        {"name": "MLBTR Transactions", "url": MLBTR_TX_FEED},
        {"name": "CBS MLB", "url": CBS_MLB_RSS},
    ]
    sources.extend(_build_google_news_sources(roster_names))

    seen = set(state.get("seen_rss_ids", []))
    patterns = _build_name_patterns(roster_names)
    matched = []

    for src in sources:
        try:
            feed = feedparser.parse(src["url"])
            entries = getattr(feed, "entries", [])[:80]
        except Exception:
            continue

        for e in entries:
            title = _normalize(getattr(e, "title", ""))
            link = _normalize(getattr(e, "link", "")) or _normalize(getattr(e, "id", ""))
            summary = _normalize(getattr(e, "summary", "")) if hasattr(e, "summary") else ""
            blob = f"{title} {summary}"

            cid = _content_id(src["name"], title, link)
            if cid in seen:
                continue

            pub_dt = None
            if hasattr(e, "published_parsed") and e.published_parsed:
                try:
                    pub_dt = datetime(*e.published_parsed[:6]).replace(tzinfo=tz.tzutc())
                except Exception:
                    pub_dt = None
            if pub_dt is None:
                pub_dt = now_utc()

            players = []
            for full, pat in patterns:
                if pat.search(blob):
                    players.append(full)

            if not players:
                continue

            for p in sorted(set(players)):
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

            seen.add(cid)

    state["seen_rss_ids"] = list(seen)[-8000:]
    return matched


# ----------------------------
# Stats helpers (Weekly)
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


def _ip_to_float(ip_val):
    if ip_val is None:
        return None
    try:
        s = str(ip_val).strip()
        if "." not in s:
            return float(s)
        whole, frac = s.split(".", 1)
        whole_f = float(whole)
        if frac == "0":
            return whole_f
        if frac == "1":
            return whole_f + (1.0 / 3.0)
        if frac == "2":
            return whole_f + (2.0 / 3.0)
        return float(s)
    except Exception:
        return None


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


def fetch_stats_splits(person_ids, group, stats_kind, sport_id, start_date=None, end_date=None):
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


def stats_tables_all_players(roster_names, name_to_id, year, weekly_start, weekly_end):
    ids = [name_to_id[n] for n in roster_names if n in name_to_id]
    ws = weekly_start.strftime("%Y-%m-%d")
    we = weekly_end.strftime("%Y-%m-%d")

    def get_group_tables(group):
        w_mlb = fetch_stats_splits(ids, group, "byDateRange", SPORT_ID_MLB, ws, we)
        s_mlb = fetch_stats_splits(ids, group, "season", SPORT_ID_MLB)
        w_milb = fetch_stats_splits(ids, group, "byDateRange", SPORT_ID_MILB, ws, we)
        s_milb = fetch_stats_splits(ids, group, "season", SPORT_ID_MILB)
        return (w_mlb, s_mlb, w_milb, s_milb)

    def splits_to_df(splits, group):
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
                ip = stat.get("inningsPitched")
                ip_f = _ip_to_float(ip)

                k_pct = _pct(so, bf) if bf not in (None, 0, "0") else None
                bb_pct = _pct(bb, bf) if bf not in (None, 0, "0") else None

                # fallback K/9 and BB/9 when BF rates aren't present (common in MiLB)
                k9 = None
                bb9 = None
                if (k_pct is None or bb_pct is None) and ip_f not in (None, 0.0):
                    try:
                        if so is not None:
                            k9 = round((float(so) * 9.0) / ip_f, 2)
                        if bb is not None:
                            bb9 = round((float(bb) * 9.0) / ip_f, 2)
                    except Exception:
                        pass

                rows.append(
                    {
                        "Name": player,
                        "Level": lvl,
                        "GS": stat.get("gamesStarted"),
                        "IP": ip,
                        "ERA": stat.get("era"),
                        "FIP": None,
                        "K%": k_pct,
                        "BB%": bb_pct,
                        "K/9": k9,
                        "BB/9": bb9,
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["Name"])
        df = pd.DataFrame(rows)

        # Choose the most "substantial" split if multiple (team changes, multiple levels, etc.)
        if group == "hitting" and "G" in df.columns:
            df["G_num"] = pd.to_numeric(df["G"], errors="coerce").fillna(0)
            df = df.sort_values(["Name", "G_num"], ascending=[True, False]).drop_duplicates("Name", keep="first")
            df = df.drop(columns=["G_num"], errors="ignore")

        if group == "pitching" and "IP" in df.columns:
            tmp = df["IP"].astype(str).str.replace(r"[^\d\.]", "", regex=True)
            df["IP_num"] = pd.to_numeric(tmp, errors="coerce").fillna(0)
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

    def merge_fill(primary, fallback, cols):
        if primary is None or primary.empty:
            base = fallback.copy()
        else:
            base = primary.copy()
            if fallback is not None and not fallback.empty:
                missing = set(fallback["Name"]) - set(base["Name"])
                if missing:
                    base = pd.concat([base, fallback[fallback["Name"].isin(missing)]], ignore_index=True)

        all_rows = pd.DataFrame({"Name": roster_names})
        base = all_rows.merge(base, on="Name", how="left")
        for c in cols:
            if c not in base.columns:
                base[c] = None
        return base

    hitter_cols = ["Level", "G", "H", "HR", "RBI", "SB", "AVG", "OBP", "wRC+", "K%", "BB%"]
    pitcher_cols = ["Level", "GS", "IP", "ERA", "FIP", "K%", "BB%", "K/9", "BB/9"]

    hitters_week = merge_fill(hitters_week_mlb, hitters_week_milb, hitter_cols)
    hitters_season = merge_fill(hitters_season_mlb, hitters_season_milb, hitter_cols)
    pitchers_week = merge_fill(pitchers_week_mlb, pitchers_week_milb, pitcher_cols)
    pitchers_season = merge_fill(pitchers_season_mlb, pitchers_season_milb, pitcher_cols)

    # FanGraphs overlay for MLB-only advanced stats
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
        print(f"[weekly] FanGraphs overlay failed: {e}")

    hitters_week = hitters_week[["Name"] + hitter_cols].sort_values("Name")
    hitters_season = hitters_season[["Name"] + hitter_cols].sort_values("Name")
    pitchers_week = pitchers_week[["Name"] + pitcher_cols].sort_values("Name")
    pitchers_season = pitchers_season[["Name"] + pitcher_cols].sort_values("Name")

    return hitters_week, hitters_season, pitchers_week, pitchers_season


# ----------------------------
# Daily
# ----------------------------
def is_daily_time(state):
    ln = local_now()
    if ln.hour != 6:
        return False
    today = ln.strftime("%Y-%m-%d")
    return state.get("last_daily_local_date") != today


def mark_daily_sent(state):
    state["last_daily_local_date"] = local_now().strftime("%Y-%m-%d")


def build_daily_bodies(official_items, reports, team_by_player, title_str):
    # group reports by player
    reports_sorted = sorted(reports, key=lambda x: (x["player"], x["source"], x["title"]))

    # plain text
    text = []
    text.append(f"Dynasty Daily Update — {title_str}")
    text.append("")
    text.append("Transaction Wire (Official)")
    if official_items:
        for it in official_items:
            nm = it["player"]
            tm = team_by_player.get(nm, "")
            hdr = f"{nm} ({tm})" if tm else nm
            text.append(f"- {hdr}: {it['desc']}")
    else:
        text.append("No official transactions.")
    text.append("")
    text.append("Reports / Quotes")
    if reports_sorted:
        cur = None
        for r in reports_sorted:
            if r["player"] != cur:
                cur = r["player"]
                tm = team_by_player.get(cur, "")
                hdr = f"{cur} ({tm})" if tm else cur
                text.append("")
                text.append(hdr)
            text.append(f"  - {r['title']} [{r['source']}] (News: {r['link']})")
    else:
        text.append("No matched reports.")
    text_body = "\n".join(text)

    # html
    html = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 10px 0;'>Dynasty Daily Update — {h(title_str)}</h2>")

    html.append("<h3 style='margin:18px 0 8px 0;'>Transaction Wire (Official)</h3>")
    if official_items:
        for it in official_items:
            nm = it["player"]
            tm = team_by_player.get(nm, "")
            hdr = f"{nm} ({tm})" if tm else nm
            html.append(
                "<div style='margin:10px 0; padding:10px 12px; border:1px solid #e8e8e8; border-radius:10px;'>"
                f"<div style='font-size:15px;'><b>{h(hdr)}</b></div>"
                f"<div style='margin-top:6px; color:#222;'>{h(it['desc'])}</div>"
                "</div>"
            )
    else:
        html.append("<div style='color:#666;'>No official transactions.</div>")

    html.append("<h3 style='margin:22px 0 8px 0;'>Reports / Quotes</h3>")
    if reports_sorted:
        cur = None
        for r in reports_sorted:
            if r["player"] != cur:
                cur = r["player"]
                tm = team_by_player.get(cur, "")
                hdr = f"{cur} ({tm})" if tm else cur
                html.append(
                    "<div style='margin:14px 0 0 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                    f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
                )
            html.append(
                "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                f"<div style='margin:0 0 5px 0;'>{h(r['title'])}</div>"
                f"<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                f"<span style='color:#555; font-size:13px;'>Source: {h(r['source'])}</span>"
                f"{news_button(r['link'])}"
                "</div>"
                "</div>"
            )
        html.append("</div>")  # close last player card
    else:
        html.append("<div style='color:#666;'>No matched reports.</div>")

    html.append("<div style='margin-top:18px; color:#777; font-size:12px;'>"
                "Items are deduped and should not repeat unless a story is reposted/updated with a new link/title."
                "</div>")
    html.append("</body></html>")
    html_body = "".join(html)

    return text_body, html_body


def run_daily(lookback_hours=None):
    state = load_state()
    roster_df = load_roster()
    roster = roster_df["player_name"].tolist()
    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    print(f"[roster] players={len(roster)} sample={roster[:10]}")

    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not is_daily_time(state):
            print("[daily] Skipping - not 6am CT (or already sent).")
            save_state(state)
            return

    since = now_utc() - timedelta(hours=lookback_hours or 24)

    official_items = []
    for player in roster:
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        tx = tx_since(fetch_transactions(pid), since)
        for t in tx:
            official_items.append({"player": player, "desc": t["desc"], "utc": t["utc"]})
            append_jsonl(WEEKLY_OFFICIAL_PATH, {"player": player, **t})

    reports = fetch_reports(roster, state)
    for r in reports:
        append_jsonl(WEEKLY_REPORTS_PATH, r)

    print(f"[daily] official_items={len(official_items)} reports_items={len(reports)}")

    if not official_items and not reports:
        if os.getenv("IS_SCHEDULED", "0") == "1":
            mark_daily_sent(state)
        save_state(state)
        return

    title_str = local_now().strftime("%b %d")
    official_items_sorted = sorted(official_items, key=lambda x: x["utc"])

    text_body, html_body = build_daily_bodies(
        official_items_sorted,
        reports,
        team_by_player,
        title_str,
    )

    send_email(f"Dynasty Daily Update — {title_str}", text_body, html_body)

    if os.getenv("IS_SCHEDULED", "0") == "1":
        mark_daily_sent(state)

    state["last_run_utc"] = now_utc().isoformat()
    save_state(state)


# ----------------------------
# Weekly (news recap + stats)
# ----------------------------
def should_send_weekly_now():
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7


def mark_weekly_sent(state):
    state["last_weekly_local_date"] = local_now().strftime("%Y-%m-%d")


def build_weekly_bodies(roster_df, official_news, reports_news, hitters_week, hitters_season, pitchers_week, pitchers_season):
    now_local = local_now()

    # weekly window: previous Mon-Sun
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (this_mon - timedelta(days=7)).date()
    end = (this_mon - timedelta(days=1)).date()

    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    # group news by player
    def group_by_player(items):
        out = {}
        for it in items:
            p = it.get("player", "")
            out.setdefault(p, []).append(it)
        for k in out:
            out[k] = sorted(out[k], key=lambda x: x.get("utc", ""))
        return out

    off_by = group_by_player(official_news)
    rep_by = group_by_player(reports_news)

    # plain text fallback (brief)
    text = []
    text.append(f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}")
    text.append(f"Weekly window: {start.strftime('%b %d')}–{end.strftime('%b %d')}")
    text.append("")
    text.append("OFFICIAL NEWS")
    if not official_news:
        text.append("No official transactions logged this week.")
    else:
        for it in official_news:
            p = it.get("player", "")
            tm = team_by_player.get(p, "")
            hdr = f"{p} ({tm})" if tm else p
            text.append(f"- {hdr}: {it.get('desc','')}")
    text.append("")
    text.append("REPORTS / QUOTES")
    if not reports_news:
        text.append("No matched reports logged this week.")
    else:
        for it in reports_news:
            p = it.get("player", "")
            tm = team_by_player.get(p, "")
            hdr = f"{p} ({tm})" if tm else p
            text.append(f"- {hdr}: {it.get('title','')} [{it.get('source','')}] {it.get('link','')}")
    text.append("")
    text.append("Stats tables included in HTML version.")
    text_body = "\n".join(text)

    # HTML
    html = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 6px 0;'>Dynasty Weekly Report — {h(now_local.strftime('%b %d, %Y'))}</h2>")
    html.append(f"<div style='color:#555; margin-bottom:14px;'>Weekly window: {h(start.strftime('%b %d'))}–{h(end.strftime('%b %d'))}</div>")

    # News recap
    html.append("<h3 style='margin:18px 0 8px 0;'>Weekly News Recap — Transaction Wire (Official)</h3>")
    if not official_news:
        html.append("<div style='color:#666;'>No official transactions logged this week.</div>")
    else:
        # player cards
        for player in sorted(off_by.keys()):
            items = off_by[player]
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
            )
            html.append("<ul style='margin:0; padding-left:18px;'>")
            for it in items:
                html.append(f"<li style='margin:6px 0;'>{h(it.get('desc',''))}</li>")
            html.append("</ul></div>")

    html.append("<h3 style='margin:22px 0 8px 0;'>Weekly News Recap — Reports / Quotes</h3>")
    if not reports_news:
        html.append("<div style='color:#666;'>No matched reports logged this week.</div>")
    else:
        for player in sorted(rep_by.keys()):
            items = rep_by[player]
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
            )
            for it in items:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(it.get('title',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#555; font-size:13px;'>Source: {h(it.get('source',''))}</span>"
                    f"{news_button(it.get('link',''))}"
                    "</div></div>"
                )
            html.append("</div>")

    # Stats
    html.append("<h3 style='margin:26px 0 8px 0;'>Weekly Stats</h3>")
    html.append("<div style='color:#555; font-size:13px; margin-bottom:10px;'>"
                "wRC+ / FIP are populated when available in FanGraphs MLB tables; otherwise blank. "
                "MiLB pitching may use K/9 and BB/9 when K%/BB% aren’t available."
                "</div>")

    html.append(render_table_html(hitters_week, "Hitters — Weekly"))
    html.append(render_table_html(pitchers_week, "Pitchers — Weekly"))

    html.append("<h3 style='margin:26px 0 8px 0;'>Season-to-date Stats</h3>")
    html.append(render_table_html(hitters_season, "Hitters — Season"))
    html.append(render_table_html(pitchers_season, "Pitchers — Season"))

    html.append("<div style='margin-top:18px; color:#777; font-size:12px;'>"
                "This report includes all news logged during the week. Daily items are deduped to avoid repeats."
                "</div>")
    html.append("</body></html>")
    html_body = "".join(html)

    return text_body, html_body


def run_weekly():
    state = load_state()

    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not should_send_weekly_now():
            print("[weekly] Skipping - not Monday 7am CT.")
            save_state(state)
            return
        # one weekly per day guard
        today = local_now().strftime("%Y-%m-%d")
        if state.get("last_weekly_local_date") == today:
            print("[weekly] Already sent today.")
            save_state(state)
            return

    roster_df = load_roster()
    roster_names = roster_df["player_name"].tolist()

    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = read_jsonl(WEEKLY_REPORTS_PATH)

    # Build stats window for previous Mon-Sun
    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    weekly_start = (this_mon - timedelta(days=7)).date()
    weekly_end = (this_mon - timedelta(days=1)).date()

    # map name->id
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
            weekly_start=weekly_start,
            weekly_end=weekly_end,
        )
    except Exception as e:
        # If stats fail, still send news recap
        print(f"[weekly] Stats error: {e}")
        hitters_week = pd.DataFrame()
        hitters_season = pd.DataFrame()
        pitchers_week = pd.DataFrame()
        pitchers_season = pd.DataFrame()

    text_body, html_body = build_weekly_bodies(
        roster_df=roster_df,
        official_news=official_news,
        reports_news=reports_news,
        hitters_week=hitters_week,
        hitters_season=hitters_season,
        pitchers_week=pitchers_week,
        pitchers_season=pitchers_season,
    )

    subject = f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}"
    send_email(subject, text_body, html_body)

    # Clear weekly logs after sending
    reset_jsonl(WEEKLY_OFFICIAL_PATH)
    reset_jsonl(WEEKLY_REPORTS_PATH)

    mark_weekly_sent(state)
    save_state(state)


# ----------------------------
# Test Modes
# ----------------------------
def run_smtp_test():
    subject = f"SMTP Test — {local_now().strftime('%b %d %I:%M %p %Z')}"
    send_email(subject, "If you received this, SMTP works.", "<b>If you received this, SMTP works.</b>")


def run_news_test():
    # Formatting-only test
    roster_df = load_roster()
    team_by = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))
    official = [{"player": roster_df["player_name"].iloc[0] if len(roster_df) else "Test Player", "desc": "Placed on IL (test)", "utc": now_utc().isoformat()}]
    reports = [{
        "utc": now_utc().isoformat(),
        "player": roster_df["player_name"].iloc[0] if len(roster_df) else "Test Player",
        "source": "Google News (Test)",
        "title": "Player working at new position this spring (test)",
        "link": "https://news.google.com/",
        "cid": "test"
    }]
    text_body, html_body = build_daily_bodies(official, reports, team_by, local_now().strftime("%b %d"))
    send_email("Daily Formatting Test", text_body, html_body)


def run_daily_realnews_test():
    run_daily(lookback_hours=24 * 14)


def run_weekly_test():
    # allow weekly regardless of day/time
    roster_df = load_roster()
    roster_names = roster_df["player_name"].tolist()

    state = load_state()
    name_to_id = {}
    for nm in roster_names:
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid
    save_state(state)

    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    weekly_start = (this_mon - timedelta(days=7)).date()
    weekly_end = (this_mon - timedelta(days=1)).date()

    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = read_jsonl(WEEKLY_REPORTS_PATH)

    year = now_local.year
    try:
        hitters_week, hitters_season, pitchers_week, pitchers_season = stats_tables_all_players(
            roster_names=roster_names,
            name_to_id=name_to_id,
            year=year,
            weekly_start=weekly_start,
            weekly_end=weekly_end,
        )
    except Exception as e:
        print(f"[weekly_test] Stats error: {e}")
        hitters_week = pd.DataFrame()
        hitters_season = pd.DataFrame()
        pitchers_week = pd.DataFrame()
        pitchers_season = pd.DataFrame()

    text_body, html_body = build_weekly_bodies(
        roster_df=roster_df,
        official_news=official_news,
        reports_news=reports_news,
        hitters_week=hitters_week,
        hitters_season=hitters_season,
        pitchers_week=pitchers_week,
        pitchers_season=pitchers_season,
    )

    send_email("Dynasty Weekly Report — TEST", text_body, html_body)


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
        raise SystemExit("Invalid RUN_MODE. Use: daily, weekly, smtp_test, news_test, daily_realnews_test, weekly_test")


if __name__ == "__main__":
    main()
