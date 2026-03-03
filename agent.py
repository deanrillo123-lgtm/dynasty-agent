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
WEEKLY_OFFICIAL_PATH = os.path.join(STATE_DIR, "weekly_official.jsonl")  # MLB transactions (player hydrate)
WEEKLY_REPORTS_PATH = os.path.join(STATE_DIR, "weekly_reports.jsonl")    # RSS matches

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

SPORT_ID_MLB = 1
SPORT_ID_MILB = 21

# MLBTR Feedburner feeds (main + transactions-only)
MLBTR_MAIN_FEED = "http://feeds.feedburner.com/MlbTradeRumors"           # :contentReference[oaicite:4]{index=4}
MLBTR_TX_FEED = "http://feeds.feedburner.com/MLBTRTransactions"          # :contentReference[oaicite:5]{index=5}

# MLB News RSS
MLB_NEWS_FEED = "https://www.mlb.com/feeds/news/rss.xml"                 # :contentReference[oaicite:6]{index=6}

# Map common team abbreviations to MLB.com URL slugs (for team RSS)
TEAM_SLUG = {
    "ARI": "dbacks",
    "ATL": "braves",
    "BAL": "orioles",
    "BOS": "redsox",
    "CHC": "cubs",
    "CWS": "whitesox",
    "CIN": "reds",
    "CLE": "guardians",
    "COL": "rockies",
    "DET": "tigers",
    "HOU": "astros",
    "KC": "royals",
    "KCR": "royals",
    "LAA": "angels",
    "LAD": "dodgers",
    "MIA": "marlins",
    "MIL": "brewers",
    "MIN": "twins",
    "NYM": "mets",
    "NYY": "yankees",
    "OAK": "athletics",
    "ATH": "athletics",
    "PHI": "phillies",
    "PIT": "pirates",
    "SD": "padres",
    "SDP": "padres",
    "SEA": "mariners",
    "SF": "giants",
    "SFG": "giants",
    "STL": "cardinals",
    "TB": "rays",
    "TBR": "rays",
    "TEX": "rangers",
    "TOR": "bluejays",
    "WSH": "nationals",
    "WAS": "nationals",
}

def team_rss_url(team_abbrev: str) -> str | None:
    ab = (team_abbrev or "").strip().upper()
    slug = TEAM_SLUG.get(ab)
    if not slug:
        return None
    return f"https://www.mlb.com/{slug}/feeds/news/rss.xml"  # pattern example :contentReference[oaicite:7]{index=7}


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
# Roster parsing (FIXED)
# ----------------------------
def _sniff_delimiter(path: str) -> str:
    # Try to detect comma vs tab, etc.
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(8192)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except Exception:
        # Heuristic: if there are many tabs, assume tab
        if sample.count("\t") > sample.count(","):
            return "\t"
        return ","


def load_roster() -> pd.DataFrame:
    """
    Returns a dataframe with:
      - player_name
      - team_abbrev (if available)
    Works for Fantrax exports that are tab-delimited and may not have headers.
    """
    delim = _sniff_delimiter(ROSTER_PATH)

    # First try: read with inferred delimiter and headers
    try:
        df = pd.read_csv(ROSTER_PATH, sep=delim, engine="python")
    except Exception:
        df = pd.read_csv(ROSTER_PATH, sep=delim, engine="python", on_bad_lines="skip")

    # If headers look wrong (e.g. first header is "*04o4s*"), re-read with header=None
    # Fantrax TSV often: ID, Pos, Name, Team, ...
    if len(df.columns) == 1 or (isinstance(df.columns[0], str) and df.columns[0].startswith("*")):
        try:
            df = pd.read_csv(ROSTER_PATH, sep=delim, engine="python", header=None, on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(ROSTER_PATH, sep=delim, engine="python", header=None)

        # Name is column 2 in your screenshot layout
        name_series = df.iloc[:, 2].astype(str).str.strip()
        team_series = df.iloc[:, 3].astype(str).str.strip() if df.shape[1] > 3 else ""
        out = pd.DataFrame({"player_name": name_series, "team_abbrev": team_series})
    else:
        # Header-based detection
        cols_lower = {c.lower(): c for c in df.columns if isinstance(c, str)}
        name_col = None
        for key in ["player", "player name", "name"]:
            if key in cols_lower:
                name_col = cols_lower[key]
                break
        if name_col is None:
            # fallback: find column that contains typical "First Last" patterns
            name_col = df.columns[0]

        team_col = None
        for key in ["team", "mlb team", "org", "organization"]:
            if key in cols_lower:
                team_col = cols_lower[key]
                break

        out = pd.DataFrame(
            {
                "player_name": df[name_col].astype(str).str.strip(),
                "team_abbrev": df[team_col].astype(str).str.strip() if team_col else "",
            }
        )

    out = out[out["player_name"].ne("")]
    out = out[~out["player_name"].str.lower().isin(["player", "player name", "name"])]

    # Fantrax sometimes includes position tags or extra stuff; strip common trailing junk
    out["player_name"] = out["player_name"].str.replace(r"\s*\(.*\)\s*$", "", regex=True).str.strip()

    out = out.drop_duplicates(subset=["player_name"]).reset_index(drop=True)
    return out


# ----------------------------
# MLBAM ID lookup
# ----------------------------
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
# OFFICIAL: MLB transactions (per player)
# ----------------------------
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
        patterns.append((full, re.compile(rf"\b{re.escape(full)}\b", re.IGNORECASE)))
        if len(last) >= 5:
            patterns.append((full, re.compile(rf"\b{re.escape(last)}\b", re.IGNORECASE)))
    return patterns


def _build_rss_sources(team_abbrevs: list[str]) -> list[dict]:
    sources = [
        {"name": "MLB.com (News)", "url": MLB_NEWS_FEED},
        {"name": "MLBTR (Main)", "url": MLBTR_MAIN_FEED},
        {"name": "MLBTR (Transactions Only)", "url": MLBTR_TX_FEED},
    ]

    # Add MLB.com team feeds for only the teams represented on your roster
    for ab in sorted(set([a.strip().upper() for a in team_abbrevs if a and str(a).strip()])):
        url = team_rss_url(ab)
        if url:
            sources.append({"name": f"MLB.com ({ab})", "url": url})

    return sources


def fetch_reports_for_roster(roster_names: list[str], team_abbrevs: list[str], state: dict, max_items_per_source: int = 50) -> list[dict]:
    rss_sources = _build_rss_sources(team_abbrevs)
    seen = state.get("seen_rss_ids", [])
    seen_set = set(seen)
    patterns = _build_name_patterns(roster_names)

    matched = []
    for src in rss_sources:
        feed = feedparser.parse(src["url"])
        entries = getattr(feed, "entries", [])[:max_items_per_source]

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

            seen_set.add(cid)

    state["seen_rss_ids"] = list(seen_set)[-4000:]
    return matched


# ----------------------------
# STATS (Weekly email)
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

def _ip_to_float(ip_val):
    # MLB Stats API often gives IP as a string like "12.1" (12 + 1/3)
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
        # fallback: treat as decimal (rare)
        return float(s)
    except Exception:
        return None

ip_f = _ip_to_float(ip)

k_pct = _pct(so, bf) if bf not in (None, 0, "0") else None
bb_pct = _pct(bb, bf) if bf not in (None, 0, "0") else None

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

        # Collapse multi-splits to most substantial
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
    pitcher_cols = ["Level", "GS", "IP", "ERA", "FIP", "K%", "BB%"]

    hitters_week = merge_fill(hitters_week_mlb, hitters_week_milb, hitter_cols)
    hitters_season = merge_fill(hitters_season_mlb, hitters_season_milb, hitter_cols)
    pitchers_week = merge_fill(pitchers_week_mlb, pitchers_week_milb, pitcher_cols)
    pitchers_season = merge_fill(pitchers_season_mlb, pitchers_season_milb, pitcher_cols)

    # FanGraphs overlay (MLB only)
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


def run_daily(lookback_hours: int | None = None):
    state = load_state()
    roster_df = load_roster()
    roster_names = roster_df["player_name"].tolist()
    team_abbrevs = roster_df["team_abbrev"].tolist() if "team_abbrev" in roster_df.columns else []

    # Gate scheduled daily sends to 6am CT
    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not is_daily_send_time(state):
            print("[daily] scheduled run but not 6am CT (or already sent). Skipping.")
            save_state(state)
            return

    # Determine "since"
    if lookback_hours is not None:
        since = now_utc() - timedelta(hours=lookback_hours)
    else:
        last_run = state.get("last_run_utc")
        if last_run:
            since = datetime.fromisoformat(last_run)
            if since.tzinfo is None:
                since = since.replace(tzinfo=tz.t
