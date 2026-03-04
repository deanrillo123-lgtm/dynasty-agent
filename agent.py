import os
import json
import smtplib
import re
import hashlib
import csv
from email.message import EmailMessage
from datetime import datetime, timedelta
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

# Fallback local roster path (kept for safety)
ROSTER_PATH = os.getenv("ROSTER_PATH", "roster.csv")

# Google Sheet config
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
ROSTER_GID = os.getenv("ROSTER_GID", "").strip()
AVAILABLE_GID = os.getenv("AVAILABLE_GID", "").strip()
DD_RANK_GID = os.getenv("DD_RANK_GID", "").strip()

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
MLB_PIPELINE_RSS = "https://www.mlb.com/pipeline/rss"
MILB_NEWS_RSS = "https://www.milb.com/feeds/news/rss.xml"
FANGRAPHS_RSS = "https://blogs.fangraphs.com/feed/"
FANGRAPHS_PROSPECTS_RSS = "https://blogs.fangraphs.com/category/prospects/feed/"
BASEBALL_AMERICA_RSS = "https://www.baseballamerica.com/feed/"
BASEBALL_PROSPECTUS_RSS = "https://www.baseballprospectus.com/feed/"
MLBTR_MAIN_FEED = "http://feeds.feedburner.com/MlbTradeRumors"
MLBTR_TX_FEED = "http://feeds.feedburner.com/MLBTRTransactions"
CBS_MLB_RSS = "https://www.cbssports.com/rss/headlines/mlb/"

INJURY_KEYWORDS = [
    "injury", "injured", "soreness", "sore", "il", "disabled list", "mri", "strain", "sprain",
    "fracture", "broken", "surgery", "shut down", "rehab", "rehabilitation", "out for", "day-to-day",
    "tightness", "discomfort", "scratched", "left the game", "stiffness"
]


# ----------------------------
# Time helpers
# ----------------------------
def local_now():
    return datetime.now(pytz.timezone(TZ_NAME))


def now_utc():
    return datetime.now(tz=tz.tzutc())


# ----------------------------
# State
# ----------------------------
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
                    "team_id_cache": {},
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
        raise RuntimeError("Missing EMAIL_ADDRESS / EMAIL_PASSWORD / RECIPIENT_EMAIL.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Dynasty Agent <{SENDER}>"
    msg["To"] = RECIPIENT

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER, SENDER_PW)
        server.send_message(msg)


# ----------------------------
# HTML helpers / UI
# ----------------------------
def h(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def section_header(title: str, color: str) -> str:
    return (
        "<div style='margin:22px 0 10px 0; display:flex; align-items:center; gap:10px;'>"
        f"<div style='width:10px; height:10px; border-radius:3px; background:{color};'></div>"
        f"<h3 style='margin:0; padding:0;'>{h(title)}</h3>"
        "</div>"
    )


def button(url: str, label: str, bg: str = "#1a73e8") -> str:
    u = (url or "").strip()
    if not u:
        return ""
    return (
        f"<a href='{h(u)}' target='_blank' rel='noopener noreferrer' "
        f"style='display:inline-block; padding:7px 10px; border-radius:8px; "
        f"background:{bg}; color:#fff; text-decoration:none; font-size:13px; font-weight:600;'>"
        f"{h(label)}</a>"
    )


def slugify_name_for_savant(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def baseball_savant_url(player_name: str, mlbam_id: int) -> str:
    slug = slugify_name_for_savant(player_name)
    return f"https://baseballsavant.mlb.com/savant-player/{slug}-{mlbam_id}"


def mlb_headshot_url(mlbam_id: int) -> str:
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_84,q_auto:best/v1/people/{mlbam_id}/headshot/67/current"


def mlb_team_logo_url(team_id: int) -> str:
    return f"https://www.mlbstatic.com/team-logos/{team_id}.png"


def render_table_html(df: pd.DataFrame, title: str, html_cols=None) -> str:
    html_cols = set(html_cols or [])
    if df is None or df.empty:
        return f"<h4 style='margin:14px 0 6px 0;'>{h(title)}</h4><div style='color:#666;'>No data.</div>"

    cols = list(df.columns)
    rows = df.fillna("").astype(str).values.tolist()

    out = []
    out.append(f"<h4 style='margin:16px 0 8px 0;'>{h(title)}</h4>")
    out.append(
        "<div style='overflow-x:auto; border:1px solid #e8e8e8; border-radius:10px;'>"
        "<table style='border-collapse:collapse; width:100%; font-size:12.5px;'>"
    )

    out.append("<thead><tr style='background:#f6f7f9;'>")
    for c in cols:
        out.append(
            "<th style='text-align:left; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
            f"{h(str(c))}</th>"
        )
    out.append("</tr></thead>")

    out.append("<tbody>")
    for i, r in enumerate(rows):
        bg = "#ffffff" if i % 2 == 0 else "#fbfbfc"
        out.append(f"<tr style='background:{bg};'>")
        for c, cell in zip(cols, r):
            cell_html = cell if c in html_cols else h(cell)
            out.append(
                "<td style='padding:7px 10px; border-bottom:1px solid #f0f0f0; white-space:nowrap;'>"
                f"{cell_html}</td>"
            )
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


# ----------------------------
# Google Sheets reading (public CSV export)
# ----------------------------
def _gsheet_csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def read_sheet_tab_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    if not sheet_id or not gid:
        raise ValueError("Missing GSHEET_ID or tab gid.")
    url = _gsheet_csv_url(sheet_id, gid)
    return pd.read_csv(url, dtype=str).fillna("")


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


# ----------------------------
# Roster + Available + Rankings
# ----------------------------
def load_roster():
    """
    Primary: Google Sheet roster paste tab (GSHEET_ID + ROSTER_GID).
    Expects a player column like: Player / player_name / Name
    Optional team column like: Team / team_abbrev
    """
    if GSHEET_ID and ROSTER_GID:
        df = read_sheet_tab_csv(GSHEET_ID, ROSTER_GID)
        player_col = _pick_col(df, ["player", "player_name", "name", "playername"])
        team_col = _pick_col(df, ["team", "team_abbrev", "teamabbr", "mlb team", "org", "organization"])

        if not player_col:
            raise RuntimeError("Roster tab must have a header like 'Player' or 'player_name'.")

        players = []
        teams = []

        for _, row in df.iterrows():
            name = str(row.get(player_col, "")).strip()
            if not name or name.lower() in ("player", "name"):
                continue
            name = re.sub(r"\s*\(.*\)$", "", name).strip()
            team = str(row.get(team_col, "")).strip() if team_col else ""
            players.append(name)
            teams.append(team)

        out = pd.DataFrame({"player_name": players, "team_abbrev": teams})
        return out.drop_duplicates(subset=["player_name"]).reset_index(drop=True)

    # Fallback: local Fantrax export format
    players = []
    teams = []

    idx_player = None
    idx_team = None

    with open(ROSTER_PATH, encoding="utf-8", errors="ignore") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row:
                continue
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


def load_available_players() -> pd.DataFrame:
    """
    Loads available/free agent players from Google Sheet tab AVAILABLE_GID.
    Expects a player column like: Player / player_name / Name
    """
    if not (GSHEET_ID and AVAILABLE_GID):
        return pd.DataFrame(columns=["player_name"])

    df = read_sheet_tab_csv(GSHEET_ID, AVAILABLE_GID)
    player_col = _pick_col(df, ["player", "player_name", "name", "playername"])
    if not player_col:
        return pd.DataFrame(columns=["player_name"])

    names = []
    for _, row in df.iterrows():
        name = str(row.get(player_col, "")).strip()
        if not name or name.lower() in ("player", "name"):
            continue
        name = re.sub(r"\s*\(.*\)$", "", name).strip()
        names.append(name)

    return pd.DataFrame({"player_name": sorted(set(names))})


def load_dynasty_dugout_rankings() -> pd.DataFrame:
    """
    Loads Dynasty Dugout rankings from Google Sheet tab DD_RANK_GID.
    Expected columns:
      - player name: Player / player_name / Name
      - rank: Rank / ranking
      - (optional) tier / notes
    """
    if not (GSHEET_ID and DD_RANK_GID):
        return pd.DataFrame(columns=["player_name", "dd_rank"])

    df = read_sheet_tab_csv(GSHEET_ID, DD_RANK_GID)

    name_col = _pick_col(df, ["player", "player_name", "name", "playername"])
    rank_col = _pick_col(df, ["rank", "ranking", "dd_rank"])
    tier_col = _pick_col(df, ["tier"])
    notes_col = _pick_col(df, ["notes", "note"])

    if not name_col or not rank_col:
        return pd.DataFrame(columns=["player_name", "dd_rank"])

    rows = []
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name or name.lower() in ("player", "name"):
            continue
        name = re.sub(r"\s*\(.*\)$", "", name).strip()

        rk_raw = str(row.get(rank_col, "")).strip()
        try:
            rk = int(float(rk_raw))
        except Exception:
            continue

        tier = str(row.get(tier_col, "")).strip() if tier_col else ""
        notes = str(row.get(notes_col, "")).strip() if notes_col else ""

        rows.append({"player_name": name, "dd_rank": rk, "tier": tier, "notes": notes})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["player_name", "dd_rank"])
    return out.sort_values("dd_rank").drop_duplicates("player_name", keep="first").reset_index(drop=True)


# ----------------------------
# MLBAM + team lookup
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


def team_id_from_abbrev(team_abbrev: str, state) -> int | None:
    ab = (team_abbrev or "").strip().upper()
    if not ab:
        return None
    cache = state.get("team_id_cache", {})
    if ab in cache:
        return cache[ab]
    try:
        res = statsapi.lookup_team(ab)
        if res and isinstance(res, list):
            tid = int(res[0].get("id"))
            cache[ab] = tid
            state["team_id_cache"] = cache
            return tid
    except Exception:
        pass
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
        {"name": "MLB Pipeline", "url": MLB_PIPELINE_RSS},
        {"name": "MiLB.com", "url": MILB_NEWS_RSS},
        {"name": "FanGraphs", "url": FANGRAPHS_RSS},
        {"name": "FanGraphs Prospects", "url": FANGRAPHS_PROSPECTS_RSS},
        {"name": "Baseball America", "url": BASEBALL_AMERICA_RSS},
        {"name": "Baseball Prospectus", "url": BASEBALL_PROSPECTUS_RSS},
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
            entries = getattr(feed, "entries", [])[:100]
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
# Weekly stats (MLB + MiLB, with FG overlay when available)
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

        # keep best split per player
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

    hw_mlb, hs_mlb, hw_milb, hs_milb = get_group_tables("hitting")
    hitters_week_mlb = splits_to_df(hw_mlb, "hitting")
    hitters_week_milb = splits_to_df(hw_milb, "hitting")
    hitters_season_mlb = splits_to_df(hs_mlb, "hitting")
    hitters_season_milb = splits_to_df(hs_milb, "hitting")

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
        print(f"[weekly] FanGraphs overlay failed: {e}")

    hitters_week = hitters_week[["Name"] + hitter_cols].sort_values("Name")
    hitters_season = hitters_season[["Name"] + hitter_cols].sort_values("Name")
    pitchers_week = pitchers_week[["Name"] + pitcher_cols].sort_values("Name")
    pitchers_season = pitchers_season[["Name"] + pitcher_cols].sort_values("Name")

    return hitters_week, hitters_season, pitchers_week, pitchers_season


def add_weekly_links_and_media(df: pd.DataFrame, roster_df: pd.DataFrame, name_to_id: dict, state: dict) -> pd.DataFrame:
    if df is None or df.empty or "Name" not in df.columns:
        return df

    team_by = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))
    df2 = df.copy()

    photos = []
    logos = []
    links = []

    for nm in df2["Name"].astype(str).tolist():
        pid = name_to_id.get(nm)
        tm_ab = team_by.get(nm, "")

        if pid:
            photos.append(f"<img src='{h(mlb_headshot_url(pid))}' width='34' height='34' style='border-radius:999px; vertical-align:middle;'/>")
        else:
            photos.append("")

        tid = team_id_from_abbrev(tm_ab, state)
        if tid:
            logos.append(f"<img src='{h(mlb_team_logo_url(tid))}' width='22' height='22' style='vertical-align:middle;'/>")
        else:
            logos.append("")

        if pid:
            links.append(button(baseball_savant_url(nm, pid), "Savant", bg="#0b8043"))
        else:
            links.append("")

    df2.insert(1, "Photo", photos)
    df2.insert(2, "Team", logos)
    df2.insert(3, "Links", links)

    return df2


# ----------------------------
# Weekly extras
# ----------------------------
def is_injury_text(s: str) -> bool:
    t = (s or "").lower()
    return any(k in t for k in INJURY_KEYWORDS)


def hot_week_sections(hitters_week: pd.DataFrame, pitchers_week: pd.DataFrame):
    hot_hitters = []
    hot_pitchers = []

    if hitters_week is not None and not hitters_week.empty:
        hw = hitters_week.copy()
        hw["HRn"] = pd.to_numeric(hw.get("HR"), errors="coerce")
        hw["SBn"] = pd.to_numeric(hw.get("SB"), errors="coerce")
        for _, r in hw.iterrows():
            hr = r.get("HRn")
            sb = r.get("SBn")
            if (pd.notna(sb) and sb >= 4) or (pd.notna(hr) and hr >= 3):
                hot_hitters.append({
                    "Name": r.get("Name"),
                    "HR": "" if pd.isna(hr) else int(hr),
                    "SB": "" if pd.isna(sb) else int(sb),
                    "Level": r.get("Level") or ""
                })

    if pitchers_week is not None and not pitchers_week.empty:
        pw = pitchers_week.copy()
        pw["ERAn"] = pd.to_numeric(pw.get("ERA"), errors="coerce")
        for _, r in pw.iterrows():
            era = r.get("ERAn")
            if pd.notna(era) and era < 1.50:
                hot_pitchers.append({
                    "Name": r.get("Name"),
                    "ERA": float(era),
                    "IP": r.get("IP") or "",
                    "Level": r.get("Level") or ""
                })

    return hot_hitters, hot_pitchers


def build_injury_watch(official_news, reports_news):
    by_player = {}
    seen_keys = set()

    for it in official_news:
        p = it.get("player", "")
        desc = it.get("desc", "")
        if p and desc and is_injury_text(desc):
            k = ("off", p, desc.strip().lower())
            if k in seen_keys:
                continue
            seen_keys.add(k)
            by_player.setdefault(p, []).append({"type": "Official", "text": desc, "link": ""})

    for it in reports_news:
        p = it.get("player", "")
        title = it.get("title", "")
        if p and title and is_injury_text(title):
            k = ("rep", p, (title.strip().lower(), it.get("link","")))
            if k in seen_keys:
                continue
            seen_keys.add(k)
            by_player.setdefault(p, []).append({"type": it.get("source", "Report"), "text": title, "link": it.get("link","")})

    return by_player


def filter_reports_excluding_injuries(reports_news, injury_by_player):
    injured_players = set(injury_by_player.keys())
    out = []
    for it in reports_news:
        p = it.get("player","")
        if p in injured_players and is_injury_text(it.get("title","")):
            continue
        out.append(it)
    return out


def best_effort_velocity_change():
    # Placeholder: keeps weekly clean until we wire statcast safely later.
    return []


# ----------------------------
# Daily schedule gate
# ----------------------------
def is_daily_time(state):
    ln = local_now()
    if ln.hour != 6:
        return False
    today = ln.strftime("%Y-%m-%d")
    return state.get("last_daily_local_date") != today


def mark_daily_sent(state):
    state["last_daily_local_date"] = local_now().strftime("%Y-%m-%d")


# ----------------------------
# Daily email build
# ----------------------------
def build_daily_bodies(official_items, reports, roster_df, title_str):
    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

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
    if reports:
        for r in reports:
            nm = r["player"]
            tm = team_by_player.get(nm, "")
            hdr = f"{nm} ({tm})" if tm else nm
            text.append(f"- {hdr}: {r['title']} [{r['source']}] {r['link']}")
    else:
        text.append("No matched reports.")
    text_body = "\n".join(text)

    html = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 8px 0;'>Dynasty Daily Update — {h(title_str)}</h2>")

    html.append(section_header("Transaction Wire (Official)", "#0b8043"))
    if official_items:
        by_player = {}
        for it in official_items:
            by_player.setdefault(it["player"], []).append(it)

        for player in sorted(by_player.keys()):
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
                "<ul style='margin:0; padding-left:18px;'>"
            )
            for it in by_player[player]:
                html.append(f"<li style='margin:6px 0;'>{h(it['desc'])}</li>")
            html.append("</ul></div>")
    else:
        html.append("<div style='color:#666;'>No official transactions.</div>")

    html.append(section_header("Reports / Quotes", "#1a73e8"))
    if reports:
        by_player = {}
        for r in reports:
            by_player.setdefault(r["player"], []).append(r)

        for player in sorted(by_player.keys()):
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
            )
            for r in by_player[player]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(r['title'])}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#555; font-size:13px;'>Source: {h(r['source'])}</span>"
                    f"{button(r['link'], 'News', bg='#1a73e8')}"
                    "</div></div>"
                )
            html.append("</div>")
    else:
        html.append("<div style='color:#666;'>No matched reports.</div>")

    html.append("</body></html>")
    html_body = "".join(html)

    return text_body, html_body


# ----------------------------
# Daily runner
# ----------------------------
def run_daily(lookback_hours=None):
    state = load_state()
    roster_df = load_roster()
    roster = roster_df["player_name"].tolist()

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

    official_items_sorted = sorted(official_items, key=lambda x: x["utc"])
    reports_sorted = sorted(reports, key=lambda x: (x["player"], x["source"], x["title"]))

    print(f"[daily] official_items={len(official_items_sorted)} reports_items={len(reports_sorted)}")

    if not official_items_sorted and not reports_sorted:
        if os.getenv("IS_SCHEDULED", "0") == "1":
            mark_daily_sent(state)
        save_state(state)
        return

    title_str = local_now().strftime("%b %d")
    text_body, html_body = build_daily_bodies(official_items_sorted, reports_sorted, roster_df, title_str)

    send_email(f"Dynasty Daily Update — {title_str}", text_body, html_body)

    if os.getenv("IS_SCHEDULED", "0") == "1":
        mark_daily_sent(state)

    state["last_run_utc"] = now_utc().isoformat()
    save_state(state)


# ----------------------------
# Weekly schedule gate
# ----------------------------
def should_send_weekly_now():
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7  # Monday 7am CT


def mark_weekly_sent(state):
    state["last_weekly_local_date"] = local_now().strftime("%Y-%m-%d")


# ----------------------------
# Weekly email build
# ----------------------------
def build_weekly_bodies(
    roster_df,
    state,
    name_to_id,
    official_news,
    reports_news,
    hitters_week,
    hitters_season,
    pitchers_week,
    pitchers_season,
):
    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (this_mon - timedelta(days=7)).date()
    end = (this_mon - timedelta(days=1)).date()

    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))
    injury_by_player = build_injury_watch(official_news, reports_news)
    hot_hitters, hot_pitchers = hot_week_sections(hitters_week, pitchers_week)
    filtered_reports = filter_reports_excluding_injuries(reports_news, injury_by_player)

    # Available prospects section (uses your sheet, filtered by Dynasty Dugout ranks)
    available_df = load_available_players()
    dd_rank_df = load_dynasty_dugout_rankings()

    top_available = pd.DataFrame()
    if not available_df.empty and not dd_rank_df.empty:
        top_available = available_df.merge(dd_rank_df, on="player_name", how="inner").sort_values("dd_rank").head(20)

    # Add media + links to stats tables
    hitters_week2 = add_weekly_links_and_media(hitters_week, roster_df, name_to_id, state)
    hitters_season2 = add_weekly_links_and_media(hitters_season, roster_df, name_to_id, state)
    pitchers_week2 = add_weekly_links_and_media(pitchers_week, roster_df, name_to_id, state)
    pitchers_season2 = add_weekly_links_and_media(pitchers_season, roster_df, name_to_id, state)

    text_body = (
        f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}\n"
        f"Weekly window: {start.strftime('%b %d')}–{end.strftime('%b %d')}\n"
    )

    html = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(
        "<div style='display:flex; justify-content:space-between; align-items:flex-end; gap:10px; flex-wrap:wrap;'>"
        f"<div><h2 style='margin:0;'>Dynasty Weekly Report — {h(now_local.strftime('%b %d, %Y'))}</h2>"
        f"<div style='color:#555; margin-top:4px;'>Weekly window: {h(start.strftime('%b %d'))}–{h(end.strftime('%b %d'))}</div></div>"
        "</div>"
    )

    # 1) Transaction wire
    html.append(section_header("Transaction Wire (Official)", "#0b8043"))
    if not official_news:
        html.append("<div style='color:#666;'>No official transactions logged this week.</div>")
    else:
        by_player = {}
        for it in official_news:
            by_player.setdefault(it.get("player", ""), []).append(it)

        for player in sorted([p for p in by_player.keys() if p]):
            items = by_player[player]
            tm = team_by_player.get(player, "")
            pid = name_to_id.get(player)
            headshot = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;'/>" if pid else ""
            tid = team_id_from_abbrev(tm, state)
            logo = f"<img src='{h(mlb_team_logo_url(tid))}' width='22' height='22' style='vertical-align:middle;'/>" if tid else ""
            hdr = f"{player} ({tm})" if tm else player

            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{headshot}"
                f"<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                f"<div style='margin-left:auto;'>{logo}</div>"
                "</div>"
                "<ul style='margin:10px 0 0 0; padding-left:18px;'>"
            )
            for it in items:
                html.append(f"<li style='margin:6px 0;'>{h(it.get('desc',''))}</li>")
            html.append("</ul></div>")

    # 2) Injuries
    html.append(section_header("Injury Watch", "#d93025"))
    if not injury_by_player:
        html.append("<div style='color:#666;'>No injuries detected in logged items this week.</div>")
    else:
        for player in sorted(injury_by_player.keys()):
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            pid = name_to_id.get(player)
            headshot = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;'/>" if pid else ""

            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #ffe3e0; background:#fff5f4; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{headshot}"
                f"<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                "</div>"
            )
            for it in injury_by_player[player]:
                link_btn = button(it.get("link",""), "News", bg="#d93025") if it.get("link") else ""
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #ffd3ce;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(it.get('text',''))}</div>"
                    f"<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#6b2b22; font-size:13px;'>Source: {h(it.get('type',''))}</span>"
                    f"{link_btn}"
                    "</div></div>"
                )
            html.append("</div>")

    # 3) Hot performances
    html.append(section_header("Hot Week Performances", "#f9ab00"))
    if not hot_hitters and not hot_pitchers:
        html.append("<div style='color:#666;'>No hot-week thresholds met (SB ≥ 4, HR ≥ 3, or ERA < 1.50).</div>")
    else:
        if hot_hitters:
            html.append("<div style='margin:8px 0 0 0; font-weight:700;'>Hitters</div>")
            html.append("<ul style='margin:6px 0 0 0; padding-left:18px;'>")
            for it in hot_hitters:
                html.append(f"<li style='margin:6px 0;'><b>{h(it['Name'])}</b> — {h(str(it.get('Level','')))} | HR: {h(str(it.get('HR','')))} | SB: {h(str(it.get('SB','')))}</li>")
            html.append("</ul>")
        if hot_pitchers:
            html.append("<div style='margin:12px 0 0 0; font-weight:700;'>Pitchers</div>")
            html.append("<ul style='margin:6px 0 0 0; padding-left:18px;'>")
            for it in hot_pitchers:
                html.append(f"<li style='margin:6px 0;'><b>{h(it['Name'])}</b> — {h(str(it.get('Level','')))} | ERA: {h(str(it.get('ERA','')))} | IP: {h(str(it.get('IP','')))}</li>")
            html.append("</ul>")

    # Optional section: Top Available (Dynasty Dugout)
    html.append(section_header("Top Available Prospects (Dynasty Dugout)", "#5f6368"))
    if top_available is None or top_available.empty:
        html.append("<div style='color:#666;'>No matches between your Available list and Dynasty Dugout rankings.</div>")
    else:
        html.append("<div style='color:#555; font-size:13px; margin-bottom:8px;'>Filtered strictly to players listed in your <b>avaliableplayers</b> tab.</div>")
        html.append("<ul style='margin:6px 0 0 0; padding-left:18px;'>")
        for _, r in top_available.iterrows():
            nm = str(r.get("player_name", ""))
            rk = str(r.get("dd_rank", ""))
            tier = str(r.get("tier", "")).strip()
            extra = f" — Tier: {h(tier)}" if tier else ""
            html.append(f"<li style='margin:6px 0;'><b>{h(nm)}</b> — Rank {h(rk)}{extra}</li>")
        html.append("</ul>")

    # 4) Stats tables (weekly + season)
    html.append(section_header("Weekly Stats", "#1a73e8"))
    html.append("<div style='color:#555; font-size:13px; margin-bottom:10px;'>"
                "wRC+ / FIP populate when FanGraphs MLB tables have them; otherwise blank. "
                "MiLB pitching may use K/9 and BB/9 when K%/BB% aren’t available."
                "</div>")
    html.append(render_table_html(hitters_week2, "Hitters — Weekly", html_cols={"Photo", "Team", "Links"}))
    html.append(render_table_html(pitchers_week2, "Pitchers — Weekly", html_cols={"Photo", "Team", "Links"}))

    html.append(section_header("Season-to-date Stats", "#1a73e8"))
    html.append(render_table_html(hitters_season2, "Hitters — Season", html_cols={"Photo", "Team", "Links"}))
    html.append(render_table_html(pitchers_season2, "Pitchers — Season", html_cols={"Photo", "Team", "Links"}))

    # 5) Major news
    html.append(section_header("Major News From The Week", "#5f6368"))
    if not filtered_reports:
        html.append("<div style='color:#666;'>No matched reports logged this week.</div>")
    else:
        by_player = {}
        for r in sorted(filtered_reports, key=lambda x: (x.get("player",""), x.get("source",""), x.get("title",""))):
            by_player.setdefault(r.get("player",""), []).append(r)

        for player in sorted([p for p in by_player.keys() if p]):
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            pid = name_to_id.get(player)
            headshot = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;'/>" if pid else ""
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{headshot}"
                f"<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                "</div>"
            )
            for r in by_player[player]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(r.get('title',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#555; font-size:13px;'>Source: {h(r.get('source',''))}</span>"
                    f"{button(r.get('link',''), 'News', bg='#5f6368')}"
                    "</div></div>"
                )
            html.append("</div>")

    # 6) Velocity placeholder
    html.append(section_header("Velocity Change Tracker", "#0b8043"))
    velo = best_effort_velocity_change()
    if not velo:
        html.append("<div style='color:#666;'>Velocity section is ready — we’ll wire Statcast safely once regular season data is flowing.</div>")

    html.append("</body></html>")
    html_body = "".join(html)

    return text_body, html_body


# ----------------------------
# Weekly runner
# ----------------------------
def run_weekly():
    state = load_state()

    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not should_send_weekly_now():
            print("[weekly] Skipping - not Monday 7am CT.")
            save_state(state)
            return
        today = local_now().strftime("%Y-%m-%d")
        if state.get("last_weekly_local_date") == today:
            print("[weekly] Already sent today.")
            save_state(state)
            return

    roster_df = load_roster()
    roster_names = roster_df["player_name"].tolist()

    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = read_jsonl(WEEKLY_REPORTS_PATH)

    now_local = local_now()
    this_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    weekly_start = (this_mon - timedelta(days=7)).date()
    weekly_end = (this_mon - timedelta(days=1)).date()

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
        print(f"[weekly] Stats error: {e}")
        hitters_week = pd.DataFrame()
        hitters_season = pd.DataFrame()
        pitchers_week = pd.DataFrame()
        pitchers_season = pd.DataFrame()

    text_body, html_body = build_weekly_bodies(
        roster_df=roster_df,
        state=state,
        name_to_id=name_to_id,
        official_news=official_news,
        reports_news=reports_news,
        hitters_week=hitters_week,
        hitters_season=hitters_season,
        pitchers_week=pitchers_week,
        pitchers_season=pitchers_season,
    )

    subject = f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}"
    send_email(subject, text_body, html_body)

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


def run_daily_realnews_test():
    run_daily(lookback_hours=24 * 14)


def run_weekly_test():
    # Run weekly regardless of day/time
    state = load_state()
    roster_df = load_roster()
    roster_names = roster_df["player_name"].tolist()

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
        state=state,
        name_to_id=name_to_id,
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
    elif mode == "daily_realnews_test":
        run_daily_realnews_test()
    elif mode == "weekly_test":
        run_weekly_test()
    elif mode == "daily":
        run_daily()
    elif mode == "weekly":
        run_weekly()
    else:
        raise SystemExit("Invalid RUN_MODE. Use: daily, weekly, smtp_test, daily_realnews_test, weekly_test")


if __name__ == "__main__":
    main()
