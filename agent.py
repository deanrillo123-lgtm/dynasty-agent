import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from dateutil import tz
import pytz
import pandas as pd

import statsapi  # MLB-StatsAPI
from pybaseball import batting_stats, pitching_stats  # FanGraphs tables (MLB)

TZ_NAME = "America/Chicago"

SENDER = os.getenv("EMAIL_ADDRESS", "").strip()
SENDER_PW = os.getenv("EMAIL_PASSWORD", "").strip()
RECIPIENT = os.getenv("RECIPIENT_EMAIL", "").strip()

ROSTER_PATH = os.getenv("ROSTER_PATH", "roster.csv")

STATE_DIR = "state"
STATE_PATH = os.path.join(STATE_DIR, "state.json")
WEEKLY_NEWS_PATH = os.path.join(STATE_DIR, "weekly_news.jsonl")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# We query MLB first, then MiLB umbrella, and fill missing players from MiLB results.
SPORT_ID_MLB = 1
SPORT_ID_MILB = 21


def local_now():
    return datetime.now(pytz.timezone(TZ_NAME))


def now_utc():
    return datetime.now(tz=tz.tzutc())


def ensure_state():
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(STATE_PATH):
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"last_run_utc": None, "player_cache": {}}, f, indent=2)
    if not os.path.exists(WEEKLY_NEWS_PATH):
        with open(WEEKLY_NEWS_PATH, "w", encoding="utf-8") as f:
            f.write("")


def load_state():
    ensure_state()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


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


def load_roster() -> pd.DataFrame:
    # Fantrax exports can have inconsistent columns/quotes -> tolerate parsing.
    try:
        df = pd.read_csv(ROSTER_PATH, engine="python")
    except Exception:
        df = pd.read_csv(ROSTER_PATH, engine="python", on_bad_lines="skip")

    # Try common columns; fallback to first column
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


def fetch_transactions(mlbam_id: int) -> list[dict]:
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
        items.append(
            {
                "utc": t_dt.isoformat(),
                "desc": desc,
                "type": t.get("typeCode") or t.get("type") or "",
            }
        )
    return items


def append_weekly_news(player: str, items: list[dict]):
    if not items:
        return
    ensure_state()
    with open(WEEKLY_NEWS_PATH, "a", encoding="utf-8") as f:
        for it in items:
            it2 = {"player": player, **it}
            f.write(json.dumps(it2) + "\n")


def read_weekly_news() -> list[dict]:
    ensure_state()
    out = []
    with open(WEEKLY_NEWS_PATH, "r", encoding="utf-8") as f:
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


def reset_weekly_news():
    with open(WEEKLY_NEWS_PATH, "w", encoding="utf-8") as f:
        f.write("")


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
    # Best-effort "Level" label from MLB Stats API split metadata.
    sport = split.get("sport") or {}
    league = split.get("league") or {}
    team = split.get("team") or {}

    # Prefer sport abbreviation/name for MiLB levels (e.g., "AAA", "AA", etc.)
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
    """
    Calls MLB Stats API /stats endpoint via statsapi wrapper.
    group: "hitting" or "pitching"
    stats_kind: "season" or "byDateRange"
    """
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
        # shape: {"stats":[{"splits":[...]}]}
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
    """
    Returns 4 dataframes:
      hitters_week, hitters_season, pitchers_week, pitchers_season
    Includes all roster players (rows exist even if stats missing).
    """
    ids = [name_to_id[n] for n in roster_names if n in name_to_id]

    ws = weekly_start.strftime("%Y-%m-%d")
    we = weekly_end.strftime("%Y-%m-%d")

    def get_group_tables(group: str):
        # Query MLB first
        w_mlb = fetch_stats_splits(ids, group, "byDateRange", SPORT_ID_MLB, ws, we)
        s_mlb = fetch_stats_splits(ids, group, "season", SPORT_ID_MLB)

        # Query MiLB umbrella and fill missing players
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
                        "K%": _pct(so, bf) if bf is not None else None,
                        "BB%": _pct(bb, bf) if bf is not None else None,
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["Name"])
        df = pd.DataFrame(rows)

        # If a player has multiple splits (multi-level), keep the row with the "best" level label
        # and non-null stats (simple heuristic: max games / IP).
        if group == "hitting" and "G" in df.columns:
            df["G_num"] = pd.to_numeric(df["G"], errors="coerce").fillna(0)
            df = df.sort_values(["Name", "G_num"], ascending=[True, False]).drop_duplicates("Name", keep="first")
            df = df.drop(columns=["G_num"], errors="ignore")
        if group == "pitching" and "IP" in df.columns:
            # IP can be string like "12.1" -> numeric-ish
            df["IP_num"] = pd.to_numeric(df["IP"], errors="coerce").fillna(0)
            df = df.sort_values(["Name", "IP_num"], ascending=[True, False]).drop_duplicates("Name", keep="first")
            df = df.drop(columns=["IP_num"], errors="ignore")

        return df

    # HIT
    hw_mlb, hs_mlb, hw_milb, hs_milb = get_group_tables("hitting")
    hitters_week_mlb = splits_to_df(hw_mlb, "hitting")
    hitters_week_milb = splits_to_df(hw_milb, "hitting")
    hitters_season_mlb = splits_to_df(hs_mlb, "hitting")
    hitters_season_milb = splits_to_df(hs_milb, "hitting")

    # PIT
    pw_mlb, ps_mlb, pw_milb, ps_milb = get_group_tables("pitching")
    pitchers_week_mlb = splits_to_df(pw_mlb, "pitching")
    pitchers_week_milb = splits_to_df(pw_milb, "pitching")
    pitchers_season_mlb = splits_to_df(ps_mlb, "pitching")
    pitchers_season_milb = splits_to_df(ps_milb, "pitching")

    def merge_fill(primary: pd.DataFrame, fallback: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        # primary preferred; fill missing names from fallback
        if primary is None or primary.empty:
            base = fallback.copy()
        else:
            base = primary.copy()
            if fallback is not None and not fallback.empty:
                missing = set(fallback["Name"]) - set(base["Name"])
                if missing:
                    base = pd.concat([base, fallback[fallback["Name"].isin(missing)]], ignore_index=True)

        # Ensure all roster names appear (even if blank stats)
        all_rows = pd.DataFrame({"Name": roster_names})
        base = all_rows.merge(base, on="Name", how="left")

        # Ensure columns exist
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

    # Overlay FanGraphs advanced season stats (MLB) when available
    try:
        fg_bat = batting_stats(year)
        fg_pit = pitching_stats(year)

        fg_bat = fg_bat[["Name", "wRC+"]].copy() if "wRC+" in fg_bat.columns else fg_bat[["Name"]].copy()
        fg_pit = fg_pit[["Name", "FIP"]].copy() if "FIP" in fg_pit.columns else fg_pit[["Name"]].copy()

        hitters_season = hitters_season.merge(fg_bat, on="Name", how="left", suffixes=("", "_fg"))
        if "wRC+_fg" in hitters_season.columns:
            hitters_season["wRC+"] = hitters_season["wRC+_fg"].combine_first(hitters_season["wRC+"])
            hitters_season = hitters_season.drop(columns=["wRC+_fg"])

        pitchers_season = pitchers_season.merge(fg_pit, on="Name", how="left", suffixes=("", "_fg"))
        if "FIP_fg" in pitchers_season.columns:
            pitchers_season["FIP"] = pitchers_season["FIP_fg"].combine_first(pitchers_season["FIP"])
            pitchers_season = pitchers_season.drop(columns=["FIP_fg"])

        # Weekly wRC+/FIP typically not available as a clean table -> leave blank
    except Exception as e:
        # If FanGraphs pull fails, keep blanks
        print(f"[stats] FanGraphs overlay failed: {e}")

    # Nice ordering
    hitters_week = hitters_week[["Name"] + hitter_cols].sort_values("Name")
    hitters_season = hitters_season[["Name"] + hitter_cols].sort_values("Name")
    pitchers_week = pitchers_week[["Name"] + pitcher_cols].sort_values("Name")
    pitchers_season = pitchers_season[["Name"] + pitcher_cols].sort_values("Name")

    return hitters_week, hitters_season, pitchers_week, pitchers_season


def run_daily_news():
    state = load_state()
    roster = load_roster()

    last_run = state.get("last_run_utc")
    if last_run:
        since = datetime.fromisoformat(last_run)
        if since.tzinfo is None:
            since = since.replace(tzinfo=tz.tzutc())
    else:
        since = now_utc() - timedelta(hours=24)

    found = []
    for player in roster["player_name"].tolist():
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        items = tx_since(fetch_transactions(pid), since)
        if items:
            append_weekly_news(player, items)
            for it in items:
                found.append((player, it["desc"], it["utc"]))

    state["last_run_utc"] = now_utc().isoformat()
    save_state(state)

    print(f"[daily] since={since.isoformat()} found_items={len(found)} roster={len(roster)}")

    # QUIET MODE: no email if no news
    if not found:
        return

    body_lines = [f"# Dynasty Daily News ({local_now().strftime('%a %b %d')})", ""]
    body_lines.append("## Updates since last check")
    for player, desc, utc in sorted(found, key=lambda x: x[2]):
        body_lines.append(f"- **{player}** — {desc}")
    body_lines.append("")
    send_email(
        subject=f"Dynasty Daily News — {local_now().strftime('%b %d')}",
        body="\n".join(body_lines),
    )


def build_weekly_email_body(force: bool = False) -> str:
    roster = load_roster()
    news = read_weekly_news()

    # Weekly window: previous Mon..Sun (local)
    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (this_mon - timedelta(days=7)).date()
    end = (this_mon - timedelta(days=1)).date()

    body = []
    body.append(f"# Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}")
    body.append(f"_Weekly window: {start.strftime('%b %d')}–{end.strftime('%b %d')}_")
    body.append("")

    body.append("## Weekly News Recap")
    if not news:
        body.append("No major news logged this week.")
    else:
        for it in news:
            try:
                dt_utc = datetime.fromisoformat(it["utc"].replace("Z", "+00:00"))
                dt_local = dt_utc.astimezone(pytz.timezone(TZ_NAME))
                ts = dt_local.strftime("%a %b %d")
            except Exception:
                ts = it.get("utc", "")
            body.append(f"- **{it.get('player','')}** — {it.get('desc','')} ({ts})")

    body.append("")
    body.append("## Weekly Stats (All Players)")
    body.append("_wRC+ / FIP will be blank for players not available in FanGraphs MLB tables._")
    body.append("")

    # Map names -> mlbam ids
    state = load_state()
    roster_names = roster["player_name"].tolist()
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


def should_send_weekly_now() -> bool:
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7  # Monday 7am CT


def run_weekly():
    if not should_send_weekly_now():
        print("[weekly] gate not met (not Monday 7am CT).")
        return

    body = build_weekly_email_body()
    send_email(
        subject=f"Dynasty Weekly Report — {local_now().strftime('%b %d, %Y')}",
        body=body,
    )
    reset_weekly_news()


def run_news_test():
    send_email(
        subject="Dynasty Daily News — TEST",
        body="# Dynasty Daily News (TEST)\n\n- **Test Player** — Placed on IL (TEST)\n- **Test Prospect** — Promoted to AAA (TEST)\n",
    )


def run_weekly_test():
    body = build_weekly_email_body(force=True)
    send_email(subject="Dynasty Weekly Report — TEST", body=body)


def run_smtp_test():
    send_email(
        subject=f"Dynasty Agent SMTP Test — {local_now().strftime('%b %d %I:%M %p %Z')}",
        body="If you received this, GitHub Actions + Gmail SMTP secrets are working.",
    )


def main():
    ensure_state()
    mode = os.getenv("RUN_MODE", "daily").strip().lower()
    print(f"[main] RUN_MODE={mode}")

    if mode == "smtp_test":
        run_smtp_test()
    elif mode == "news_test":
        run_news_test()
    elif mode == "weekly_test":
        run_weekly_test()
    elif mode == "daily":
        run_daily_news()
    elif mode == "weekly":
        run_weekly()
    else:
        raise SystemExit("RUN_MODE must be one of: daily, weekly, smtp_test, news_test, weekly_test")


if __name__ == "__main__":
    main()
