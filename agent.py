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
import requests
import feedparser
import statsapi
from pybaseball import batting_stats, pitching_stats
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
TZ_NAME = "America/Chicago"

SENDER = os.getenv("EMAIL_ADDRESS", "").strip()
SENDER_PW = os.getenv("EMAIL_PASSWORD", "").strip()
RECIPIENT = os.getenv("RECIPIENT_EMAIL", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
ROSTER_GID = os.getenv("ROSTER_GID", "").strip()
AVAILABLE_GID = os.getenv("AVAILABLE_GID", "").strip()
DD_RANK_GID = os.getenv("DD_RANK_GID", "").strip()
BP_RANK_GID = os.getenv("BP_RANK_GID", "").strip()
TOP500_GID = os.getenv("TOP500_GID", "").strip()

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
PITCHERLIST_RSS = "https://pitcherlist.com/feed/"

# Daily MLB Adds inclusion days: Sunday(6), Wednesday(2), Saturday(5)
MIDWEEK_ADDS_WEEKDAYS = {6, 2, 5}

INJURY_KEYWORDS = [
    "injury", "injured", "soreness", "sore", "il", "disabled list", "mri", "strain", "sprain",
    "fracture", "broken", "surgery", "shut down", "rehab", "rehabilitation", "out for", "day-to-day",
    "tightness", "discomfort", "scratched", "left the game", "stiffness"
]

OPPORTUNITY_KEYWORDS = [
    "expected to start", "in line for", "more playing time", "bigger role", "everyday role",
    "role increase", "vacancy", "opening", "starting job", "wins job", "takes over", "replacing",
    "fill in", "fill-in", "will see time", "gets time", "moving into rotation", "named the starter",
    "promoted", "called up", "call-up", "optioned", "demoted", "sent down", "dfa", "designated for assignment",
    "traded", "trade", "acquired", "moved", "suspended", "suspension", "out indefinitely"
]

ASIA_HINTS = ["npb", "kbo", "cpbl", "japan", "korea", "taiwan", "nippon"]

MLB_HITTER_POS_ORDER = ["C", "1B", "2B", "SS", "3B", "OF", "DH"]

# =========================
# Time helpers
# =========================
def local_now():
    return datetime.now(pytz.timezone(TZ_NAME))

def now_utc():
    return datetime.now(tz=tz.tzutc())

# =========================
# State helpers
# =========================
def ensure_state_files():
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(STATE_PATH):
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({
                "player_cache": {},
                "seen_rss_ids": [],
                "last_daily_local_date": None,
                "last_weekly_local_date": None,
                "team_abbrev_map": {},
                "cache_files": {},
            }, f, indent=2)

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

# =========================
# Email
# =========================
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

# =========================
# HTML helpers
# =========================
def h(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

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

def render_table_html(df: pd.DataFrame, title: str, html_cols=None) -> str:
    html_cols = set(html_cols or [])
    if df is None or df.empty:
        return f"<h4 style='margin:14px 0 6px 0;'>{h(title)}</h4><div style='color:#666;'>No data.</div>"

    cols = list(df.columns)
    rows = df.fillna("").astype(str).values.tolist()

    out = []
    out.append(f"<h4 style='margin:16px 0 8px 0;'>{h(title)}</h4>")
    out.append("<div style='overflow-x:auto; border:1px solid #e8e8e8; border-radius:10px;'>")
    out.append("<table style='border-collapse:collapse; width:100%; font-size:12.5px;'>")

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
            out.append("<td style='padding:7px 10px; border-bottom:1px solid #f0f0f0; white-space:nowrap;'>"
                       f"{cell_html}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)

# =========================
# URLs
# =========================
def mlb_headshot_url(mlbam_id: int) -> str:
    return f"https://content.mlb.com/images/headshots/current/60x60/{mlbam_id}.png"

def mlb_team_logo_url(team_id: int) -> str:
    return f"https://www.mlbstatic.com/team-logos/{team_id}.png"

def baseball_savant_url(mlbam_id: int) -> str:
    return f"https://baseballsavant.mlb.com/savant-player/{mlbam_id}"

# =========================
# Google Sheets
# =========================
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

def _norm_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s*\(.*\)$", "", name).strip()
    return re.sub(r"\s+", " ", name)

def _first_pos_for_sort(pos_str: str) -> str:
    s = (pos_str or "").strip()
    if not s:
        return ""
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    return parts[0] if parts else s.strip().upper()

def _pos_sort_key_mlb(pos_str: str) -> int:
    p = _first_pos_for_sort(pos_str)
    if p in MLB_HITTER_POS_ORDER:
        return MLB_HITTER_POS_ORDER.index(p)
    return 999

def is_pitcher_position(pos_str: str) -> bool:
    p = (pos_str or "").upper()
    return ("P" == p) or ("SP" in p) or ("RP" in p) or p.startswith("P,")

def parse_positions_of_need_from_roster(roster_df: pd.DataFrame) -> list[str]:
    if roster_df is None or roster_df.empty:
        return []
    col = None
    for c in roster_df.columns:
        if c.strip().lower() == "positions of need":
            col = c
            break
    if not col:
        return []

    val = ""
    for x in roster_df[col].astype(str).tolist():
        if x and x.strip():
            val = x.strip()
            break
    if not val:
        return []

    raw = re.split(r"[;,/]\s*|\s*\|\s*", val)
    if len(raw) == 1:
        raw = [p.strip() for p in val.split(",")]

    cleaned = []
    for p in raw:
        p2 = p.strip().upper()
        if not p2:
            continue
        p2 = p2.replace("LF", "OF").replace("CF", "OF").replace("RF", "OF")
        cleaned.append(p2)

    out = []
    for p in cleaned:
        if p not in out:
            out.append(p)
    return out

def load_roster() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, ROSTER_GID)
    player_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    team_col = _pick_col(df, ["team", "team_abbrev", "teamabbr", "mlb team", "org", "organization"])
    pos_col = _pick_col(df, ["position", "pos"])
    age_col = _pick_col(df, ["age"])

    if not player_col:
        raise RuntimeError("Roster tab must have a header like 'Player' or 'player_name'.")

    out_rows = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(player_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        out_rows.append({
            "player_name": name,
            "team_abbrev": str(row.get(team_col, "")).strip() if team_col else "",
            "position": str(row.get(pos_col, "")).strip() if pos_col else "",
            "age": str(row.get(age_col, "")).strip() if age_col else "",
        })

    out = pd.DataFrame(out_rows)
    return out.drop_duplicates(subset=["player_name"]).reset_index(drop=True)

def load_available_players() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, AVAILABLE_GID)
    player_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    team_col = _pick_col(df, ["team", "team_abbrev", "teamabbr", "mlb team", "org", "organization"])
    pos_col = _pick_col(df, ["position", "pos"])
    age_col = _pick_col(df, ["age"])

    if not player_col:
        return pd.DataFrame(columns=["player_name", "team_abbrev", "position", "age"])

    rows = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(player_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rows.append({
            "player_name": name,
            "team_abbrev": str(row.get(team_col, "")).strip() if team_col else "",
            "position": str(row.get(pos_col, "")).strip() if pos_col else "",
            "age": str(row.get(age_col, "")).strip() if age_col else "",
        })
    return pd.DataFrame(rows).drop_duplicates(subset=["player_name"]).reset_index(drop=True)

def load_dynasty_dugout_rankings() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, DD_RANK_GID)
    name_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    rank_col = _pick_col(df, ["rank", "ranking", "dd_rank"])
    signed_col = _pick_col(df, ["signed", "signed_year", "signed year", "signed/drafted"])

    if not name_col or not rank_col:
        return pd.DataFrame(columns=["player_name", "dd_rank", "signed_year"])

    rows = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(name_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rk_raw = str(row.get(rank_col, "")).strip()
        try:
            rk = int(float(rk_raw))
        except Exception:
            continue

        signed_year = None
        if signed_col:
            sy = str(row.get(signed_col, "")).strip()
            m = re.search(r"(19|20)\d{2}", sy)
            if m:
                try:
                    signed_year = int(m.group(0))
                except Exception:
                    signed_year = None

        rows.append({"player_name": name, "dd_rank": rk, "signed_year": signed_year})

    out = pd.DataFrame(rows)
    return out.sort_values("dd_rank").drop_duplicates("player_name", keep="first").reset_index(drop=True)

def load_baseball_prospectus_rankings() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, BP_RANK_GID)
    name_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    rank_col = _pick_col(df, ["rank", "ranking", "bp_rank"])

    if not name_col or not rank_col:
        return pd.DataFrame(columns=["player_name", "bp_rank"])

    rows = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(name_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rk_raw = str(row.get(rank_col, "")).strip()
        try:
            rk = int(float(rk_raw))
        except Exception:
            continue
        rows.append({"player_name": name, "bp_rank": rk})

    out = pd.DataFrame(rows)
    return out.sort_values("bp_rank").drop_duplicates("player_name", keep="first").reset_index(drop=True)

def load_top500_dynasty_rankings() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, TOP500_GID)
    name_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    rank_col = _pick_col(df, ["rank", "ranking", "top500_rank", "top500_rankings", "top 500", "top500"])

    if not name_col or not rank_col:
        return pd.DataFrame(columns=["player_name", "top500_rank"])

    rows = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(name_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rk_raw = str(row.get(rank_col, "")).strip()
        try:
            rk = int(float(rk_raw))
        except Exception:
            continue
        rows.append({"player_name": name, "top500_rank": rk})

    out = pd.DataFrame(rows)
    return out.sort_values("top500_rank").drop_duplicates("player_name", keep="first").reset_index(drop=True)

# =========================
# MLBAM / team maps
# =========================
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

def build_team_abbrev_map(state) -> dict:
    cache = state.get("team_abbrev_map")
    if isinstance(cache, dict) and cache:
        return cache
    mapping = {}
    try:
        teams = statsapi.get("teams", {"sportId": 1}).get("teams", [])
        for t in teams:
            ab = (t.get("abbreviation") or "").upper().strip()
            tid = t.get("id")
            if ab and tid:
                mapping[ab] = int(tid)
    except Exception:
        pass
    state["team_abbrev_map"] = mapping
    return mapping

def team_id_from_abbrev(team_abbrev: str, state) -> int | None:
    ab = (team_abbrev or "").strip().upper()
    if not ab:
        return None
    mapping = build_team_abbrev_map(state)
    return mapping.get(ab)

# =========================
# Transactions
# =========================
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

# =========================
# RSS / News matching
# =========================
def _normalize(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

def _content_id(source, title, link):
    return hashlib.sha1(f"{source}|{title}|{link}".encode("utf-8", errors="ignore")).hexdigest()

def _build_name_patterns(names):
    return [(name, re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)) for name in names]

def _google_news_url(query):
    from urllib.parse import quote_plus
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"

def _build_google_news_sources(names):
    sources = []
    chunk_size = 8
    for i in range(0, len(names), chunk_size):
        chunk = names[i:i+chunk_size]
        or_part = " OR ".join([f"\"{n}\"" for n in chunk])
        query = f"({or_part}) baseball (injury OR soreness OR IL OR optioned OR promoted OR demoted OR trade OR traded OR DFA OR rehab OR suspension OR role)"
        sources.append({"name": f"Google News (Roster {i//chunk_size+1})", "url": _google_news_url(query)})

    sources.append({"name": "Google News (CBS Fantasy)", "url": _google_news_url("site:cbssports.com/fantasy baseball")})
    sources.append({"name": "Google News (RotoBaller)", "url": _google_news_url("site:rotoballer.com baseball")})
    sources.append({"name": "Google News (Pitcher List)", "url": _google_news_url("site:pitcherlist.com baseball")})
    sources.append({"name": "Google News (@pitcherlistplv)", "url": _google_news_url('"pitcherlistplv"')})
    return sources

def fetch_reports(names, state):
    sources = [
        {"name": "MLB.com", "url": MLB_NEWS_FEED},
        {"name": "MLB Pipeline", "url": MLB_PIPELINE_RSS},
        {"name": "MiLB.com", "url": MILB_NEWS_RSS},
        {"name": "FanGraphs", "url": FANGRAPHS_RSS},
        {"name": "FanGraphs Prospects", "url": FANGRAPHS_PROSPECTS_RSS},
        {"name": "Baseball America", "url": BASEBALL_AMERICA_RSS},
        {"name": "Baseball Prospectus", "url": BASEBALL_PROSPECTUS_RSS},
        {"name": "Pitcher List", "url": PITCHERLIST_RSS},
        {"name": "MLBTR Main", "url": MLBTR_MAIN_FEED},
        {"name": "MLBTR Transactions", "url": MLBTR_TX_FEED},
        {"name": "CBS MLB", "url": CBS_MLB_RSS},
    ]
    sources.extend(_build_google_news_sources(names))

    seen = set(state.get("seen_rss_ids", []))
    patterns = _build_name_patterns(names)
    matched = []

    for src in sources:
        try:
            feed = feedparser.parse(src["url"])
            entries = getattr(feed, "entries", [])[:120]
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
                matched.append({
                    "utc": pub_dt.isoformat(),
                    "player": p,
                    "source": src["name"],
                    "title": title,
                    "link": link,
                    "cid": cid,
                })
            seen.add(cid)

    state["seen_rss_ids"] = list(seen)[-8000:]
    return matched

# =========================
# Opportunity & injury detection
# =========================
def is_injury_text(s: str) -> bool:
    t = (s or "").lower()
    return any(k in t for k in INJURY_KEYWORDS)

def is_opportunity_text(s: str) -> bool:
    t = (s or "").lower()
    return any(k in t for k in OPPORTUNITY_KEYWORDS)

def opportunity_confidence(title: str) -> str:
    t = (title or "").lower()
    # HIGH: direct role change signals
    high = ["placed on il", "out for", "suspended", "traded", "called up", "promoted", "moving into rotation", "named the starter", "everyday role"]
    if any(x in t for x in high):
        return "HIGH"
    # MEDIUM: likely but softer
    med = ["expected to", "in line for", "more playing time", "opening", "vacancy", "replacing", "takes over"]
    if any(x in t for x in med):
        return "MEDIUM"
    return "LOW"

def compute_opportunity_signals(items: list[dict], lookback_days: int = 14) -> dict:
    cutoff = now_utc() - timedelta(days=lookback_days)
    out = {}
    for it in items:
        try:
            utc = datetime.fromisoformat(it.get("utc", "").replace("Z", "+00:00"))
        except Exception:
            continue
        if utc < cutoff:
            continue

        player = it.get("player")
        title = it.get("title", "") or it.get("desc", "") or ""
        link = it.get("link", "") or ""
        if not player or not title:
            continue

        if is_opportunity_text(title):
            d = out.setdefault(player, {"count": 0, "notes": [], "links": [], "confidence": []})
            d["count"] += 1
            if len(d["notes"]) < 3:
                d["notes"].append(title)
                d["links"].append(link)
                d["confidence"].append(opportunity_confidence(title))
    return out

# =========================
# Daily gate & starters
# =========================
def is_daily_time(state):
    ln = local_now()
    if ln.hour != 6:
        return False
    today = ln.strftime("%Y-%m-%d")
    return state.get("last_daily_local_date") != today

def mark_daily_sent(state):
    state["last_daily_local_date"] = local_now().strftime("%Y-%m-%d")

def should_include_midweek_adds_now() -> bool:
    ln = local_now()
    return ln.hour == 6 and ln.weekday() in MIDWEEK_ADDS_WEEKDAYS

def todays_starters_for_roster(roster_df: pd.DataFrame):
    roster_df = roster_df.copy()
    roster_df["position"] = roster_df.get("position", "").fillna("").astype(str)
    sp_names = roster_df.loc[roster_df["position"].str.contains("SP", case=False, na=False), "player_name"].tolist()
    sp_set = set(sp_names)
    if not sp_set:
        return []

    today_str = local_now().date().strftime("%Y-%m-%d")
    try:
        games = statsapi.schedule(date=today_str, sportId=1)
    except Exception:
        return []

    out = []
    for g in games:
        game_dt_utc = g.get("game_datetime")
        first_pitch_ct = ""
        if game_dt_utc:
            try:
                dt = datetime.fromisoformat(game_dt_utc.replace("Z", "+00:00"))
                ct = dt.astimezone(pytz.timezone(TZ_NAME))
                first_pitch_ct = ct.strftime("%-I:%M %p %Z")
            except Exception:
                first_pitch_ct = g.get("game_time", "") or ""
        away = g.get("away_name", "")
        home = g.get("home_name", "")
        away_pp = (g.get("away_probable_pitcher") or "").strip()
        home_pp = (g.get("home_probable_pitcher") or "").strip()

        if away_pp in sp_set:
            out.append({"player": away_pp, "home_away": "at", "opponent": home, "first_pitch_ct": first_pitch_ct})
        if home_pp in sp_set:
            out.append({"player": home_pp, "home_away": "vs", "opponent": away, "first_pitch_ct": first_pitch_ct})
    return out

# =========================
# Level inference (best effort)
# =========================
def infer_player_level(pid: int, year: int) -> str:
    try:
        mlb_hit = statsapi.get("stats", {"group": "hitting", "stats": "season", "sportId": 1, "personIds": str(pid)})
        mlb_pit = statsapi.get("stats", {"group": "pitching", "stats": "season", "sportId": 1, "personIds": str(pid)})

        def any_played(stats_blob):
            try:
                splits = stats_blob.get("stats", [])[0].get("splits", [])
                if not splits:
                    return False
                st = splits[0].get("stat", {})
                return any(float(st.get(k, 0) or 0) > 0 for k in ["plateAppearances", "inningsPitched", "gamesPlayed"])
            except Exception:
                return False

        if any_played(mlb_hit) or any_played(mlb_pit):
            return "MLB"

        milb_hit = statsapi.get("stats", {"group": "hitting", "stats": "season", "sportId": 21, "personIds": str(pid)})
        try:
            splits = milb_hit.get("stats", [])[0].get("splits", [])
            if splits:
                sp = splits[0]
                sport = sp.get("sport") or {}
                league = sp.get("league") or {}
                return (sport.get("abbreviation") or league.get("abbreviation") or league.get("name") or "").strip()
        except Exception:
            pass

        milb_pit = statsapi.get("stats", {"group": "pitching", "stats": "season", "sportId": 21, "personIds": str(pid)})
        try:
            splits = milb_pit.get("stats", [])[0].get("splits", [])
            if splits:
                sp = splits[0]
                sport = sp.get("sport") or {}
                league = sp.get("league") or {}
                return (sport.get("abbreviation") or league.get("abbreviation") or league.get("name") or "").strip()
        except Exception:
            pass
    except Exception:
        pass
    return ""

def looks_like_asia(team_str: str, level_str: str) -> bool:
    blob = f"{team_str} {level_str}".lower()
    return any(h in blob for h in ASIA_HINTS)

# =========================
# Savant leaderboards (CSV)
# =========================
def _cached_csv(state: dict, key: str):
    cf = state.get("cache_files", {})
    ent = cf.get(key, {})
    path = ent.get("path")
    fetched = ent.get("fetched_utc")
    if path and os.path.exists(path) and fetched:
        try:
            dt = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
            if (now_utc() - dt) <= timedelta(days=6):
                return path
        except Exception:
            pass
    return None

def _save_cached_csv(state: dict, key: str, path: str):
    cf = state.get("cache_files", {})
    cf[key] = {"path": path, "fetched_utc": now_utc().isoformat()}
    state["cache_files"] = cf

def fetch_savant_leaderboard(year: int, which: str, state: dict) -> pd.DataFrame:
    assert which in ("batter", "pitcher")
    cache_key = f"savant_{which}_{year}"
    cached_path = _cached_csv(state, cache_key)
    if cached_path:
        try:
            return pd.read_csv(cached_path, dtype=str).fillna("")
        except Exception:
            pass

    url = f"https://baseballsavant.mlb.com/leaderboard/statcast?type={which}&year={year}&position=&team=&min=q&csv=true"
    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        os.makedirs(STATE_DIR, exist_ok=True)
        path = os.path.join(STATE_DIR, f"{cache_key}.csv")
        with open(path, "wb") as f:
            f.write(r.content)
        _save_cached_csv(state, cache_key, path)
        save_state(state)
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()

# =========================
# Stats helpers
# =========================
def percentile_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    pct = s.rank(pct=True, method="average")
    if higher_is_better:
        return pct
    return 1 - pct

def safe_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def innings_to_float(ip):
    if ip is None or ip == "":
        return None
    s = str(ip)
    if "." not in s:
        return safe_float(s)
    a, b = s.split(".", 1)
    try:
        whole = float(a)
    except Exception:
        return None
    try:
        frac = int(b)
    except Exception:
        frac = 0
    if frac == 1:
        return whole + 1/3
    if frac == 2:
        return whole + 2/3
    return whole

def k9(so, ip):
    so = safe_float(so)
    ipf = innings_to_float(ip)
    if so is None or not ipf or ipf == 0:
        return None
    return 9 * so / ipf

def bb9(bb, ip):
    bb = safe_float(bb)
    ipf = innings_to_float(ip)
    if bb is None or not ipf or ipf == 0:
        return None
    return 9 * bb / ipf

# =========================
# StatsAPI batch queries
# =========================
def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_statsapi_by_date_range(group: str, sport_id: int, person_ids: list[int], start_date: date, end_date: date):
    out = {}
    if not person_ids:
        return out
    for chunk in _chunks(person_ids, 45):
        try:
            blob = statsapi.get(
                "stats",
                {
                    "group": group,
                    "stats": "byDateRange",
                    "sportId": sport_id,
                    "personIds": ",".join(str(x) for x in chunk),
                    "startDate": start_date.strftime("%Y-%m-%d"),
                    "endDate": end_date.strftime("%Y-%m-%d"),
                },
            )
        except Exception:
            continue

        try:
            splits = blob.get("stats", [])[0].get("splits", [])
        except Exception:
            splits = []

        for sp in splits:
            try:
                pid = int(sp.get("player", {}).get("id"))
            except Exception:
                continue
            out[pid] = sp.get("stat", {}) or {}
    return out

def fetch_statsapi_season(group: str, sport_id: int, person_ids: list[int]):
    out = {}
    if not person_ids:
        return out
    for chunk in _chunks(person_ids, 45):
        try:
            blob = statsapi.get(
                "stats",
                {
                    "group": group,
                    "stats": "season",
                    "sportId": sport_id,
                    "personIds": ",".join(str(x) for x in chunk),
                },
            )
        except Exception:
            continue

        try:
            splits = blob.get("stats", [])[0].get("splits", [])
        except Exception:
            splits = []

        for sp in splits:
            try:
                pid = int(sp.get("player", {}).get("id"))
            except Exception:
                continue
            out[pid] = sp.get("stat", {}) or {}
    return out

# =========================
# Hot week
# =========================
def hot_week_tables(roster_info: pd.DataFrame, pid_map: dict, week_hit_stats: dict, week_pit_stats: dict):
    hitters = []
    sp_list = []
    rp_list = []

    for _, r in roster_info.iterrows():
        name = r["player_name"]
        pos = r.get("position", "")
        pid = pid_map.get(name)
        if not pid:
            continue

        if not is_pitcher_position(pos):
            st = week_hit_stats.get(pid, {}) or {}
            ops = safe_float(st.get("ops"))
            hr = safe_float(st.get("homeRuns"))
            sb = safe_float(st.get("stolenBases"))
            if (hr is not None and hr >= 3) or (sb is not None and sb >= 4) or (ops is not None and ops >= 0.950):
                hitters.append({
                    "Player": name,
                    "OPS": st.get("ops",""),
                    "HR": st.get("homeRuns",""),
                    "SB": st.get("stolenBases",""),
                    "H": st.get("hits",""),
                    "RBI": st.get("rbi",""),
                })
        else:
            st = week_pit_stats.get(pid, {}) or {}
            gs = safe_float(st.get("gamesStarted"))
            era = safe_float(st.get("era"))
            sv = safe_float(st.get("saves"))
            hld = safe_float(st.get("holds"))

            if gs and gs >= 1:
                if era is not None and era < 1.50:
                    sp_list.append({
                        "Pitcher": name,
                        "GS": st.get("gamesStarted",""),
                        "IP": st.get("inningsPitched",""),
                        "ERA": st.get("era",""),
                        "SO": st.get("strikeOuts",""),
                        "BB": st.get("baseOnBalls",""),
                    })
            else:
                total = (sv or 0) + (hld or 0)
                if total >= 3:
                    rp_list.append({
                        "Reliever": name,
                        "SV": st.get("saves",""),
                        "HLD": st.get("holds",""),
                        "IP": st.get("inningsPitched",""),
                        "ERA": st.get("era",""),
                        "SO": st.get("strikeOuts",""),
                    })

    return pd.DataFrame(hitters), pd.DataFrame(sp_list), pd.DataFrame(rp_list)

# =========================
# Weekly date windows
# =========================
def previous_monday_sunday_window(now_local: datetime) -> tuple[date, date]:
    this_monday = (now_local.date() - timedelta(days=now_local.weekday()))
    prev_monday = this_monday - timedelta(days=7)
    prev_sunday = this_monday - timedelta(days=1)
    return prev_monday, prev_sunday

def week_date_range_monday_sunday(now_local: datetime):
    monday = (now_local - timedelta(days=now_local.weekday())).date()
    sunday = monday + timedelta(days=6)
    return monday, sunday

# =========================
# Two-start pitchers
# =========================
def two_start_pitchers_week(roster_df: pd.DataFrame):
    roster_df = roster_df.copy()
    roster_df["position"] = roster_df.get("position", "").fillna("").astype(str)
    sp_names = roster_df.loc[roster_df["position"].str.contains("SP", case=False, na=False), "player_name"].tolist()
    sp_set = set(sp_names)
    if not sp_set:
        return []

    now_local = local_now()
    mon, sun = week_date_range_monday_sunday(now_local)

    out_count = {n: 0 for n in sp_set}
    out_games = {n: [] for n in sp_set}

    d = mon
    while d <= sun:
        d_str = d.strftime("%Y-%m-%d")
        try:
            games = statsapi.schedule(date=d_str, sportId=1)
        except Exception:
            games = []
        for g in games:
            away_pp = (g.get("away_probable_pitcher") or "").strip()
            home_pp = (g.get("home_probable_pitcher") or "").strip()
            away = g.get("away_name", "")
            home = g.get("home_name", "")
            label = d.strftime("%a")

            if away_pp in sp_set:
                out_count[away_pp] += 1
                out_games[away_pp].append(f"{label} at {home}")
            if home_pp in sp_set:
                out_count[home_pp] += 1
                out_games[home_pp].append(f"{label} vs {away}")
        d += timedelta(days=1)

    twos = []
    for p, c in out_count.items():
        if c >= 2:
            twos.append({"player": p, "starts": c, "details": out_games.get(p, [])})
    twos.sort(key=lambda x: (-x["starts"], x["player"]))
    return twos

# =========================
# Draft-year filter logic
# =========================
def exclude_current_year_draft_pick(dd_signed_year, has_pro_evidence: bool, year: int) -> bool:
    if dd_signed_year is None:
        return False
    try:
        if int(dd_signed_year) != int(year):
            return False
    except Exception:
        return False
    # If they're truly a current-year draftee and we have no pro evidence, exclude.
    return not has_pro_evidence

# =========================
# Adds scoring: MLB + urgency
# =========================
def compute_waiver_urgency(add_score: float | None, opp_count: int, fills_need: bool, confidence: str) -> tuple[int, str]:
    """
    Returns (urgency_1_5, why_line)
    """
    score = float(add_score or 0.0)
    points = 0.0

    # Add Score tiering
    if score >= 80: points += 3.0
    elif score >= 70: points += 2.2
    elif score >= 60: points += 1.4
    elif score >= 50: points += 0.8
    else: points += 0.3

    # Opportunity mentions
    points += min(3.0, opp_count * 0.9)

    # Positions of need
    if fills_need:
        points += 1.0

    # Confidence
    if confidence == "HIGH":
        points += 0.8
    elif confidence == "MEDIUM":
        points += 0.4

    # Map to 1-5
    if points >= 6.0:
        urg = 5
    elif points >= 4.8:
        urg = 4
    elif points >= 3.5:
        urg = 3
    elif points >= 2.3:
        urg = 2
    else:
        urg = 1

    why = f"AddScore={score:.1f}"
    if opp_count:
        why += f", Opp={opp_count}"
    if fills_need:
        why += ", fills need"
    if confidence:
        why += f", {confidence}"
    return urg, why

def compute_major_league_adds(
    available_df: pd.DataFrame,
    top500_df: pd.DataFrame,
    savant_bat_df: pd.DataFrame,
    savant_pit_df: pd.DataFrame,
    recent_reports: list[dict],
    state: dict,
    year: int,
    positions_of_need: list[str],
) -> pd.DataFrame:
    if available_df is None or available_df.empty:
        return pd.DataFrame()

    cand = available_df.copy()
    if top500_df is not None and not top500_df.empty:
        cand = cand.merge(top500_df, on="player_name", how="left")

    name_to_id = {}
    for nm in cand["player_name"].tolist():
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid

    cand["pid"] = cand["player_name"].map(name_to_id)
    cand["pid_int"] = pd.to_numeric(cand["pid"], errors="coerce")
    cand["Level"] = cand["pid"].apply(lambda x: infer_player_level(int(x), year) if pd.notna(x) else "")
    cand = cand[cand["Level"].astype(str).str.upper().str.contains("MLB")].copy()
    if cand.empty:
        return pd.DataFrame()

    # Performance via FanGraphs (MLB only)
    try:
        fg_hit = batting_stats(year)[["Name", "Team", "G", "PA", "HR", "SB", "AVG", "OBP", "SLG", "OPS", "wRC+", "K%", "BB%"]].copy()
    except Exception:
        fg_hit = pd.DataFrame(columns=["Name"])

    try:
        fg_pit = pitching_stats(year)[["Name", "Team", "GS", "G", "IP", "ERA", "FIP", "K%", "BB%", "SV", "HLD"]].copy()
    except Exception:
        fg_pit = pd.DataFrame(columns=["Name"])

    out = cand.rename(columns={"player_name": "Name", "team_abbrev": "Team", "position": "Position", "age": "Age"})
    out["is_pitcher"] = out["Position"].apply(is_pitcher_position)

    hitters = out[~out["is_pitcher"]].merge(fg_hit, on="Name", how="left")
    pitchers = out[out["is_pitcher"]].merge(fg_pit, on="Name", how="left")

    # Savant join (best effort)
    def _join_savant(df, sav_df):
        if df.empty or sav_df is None or sav_df.empty:
            return df
        sav = sav_df.copy()
        cols_lower = {c.lower(): c for c in sav.columns}
        pid_col = cols_lower.get("player_id") or cols_lower.get("playerid")
        if pid_col:
            sav = sav.rename(columns={pid_col: "player_id"})
        sav["player_id_num"] = pd.to_numeric(sav.get("player_id"), errors="coerce")
        keep = ["player_id_num"]
        # keep some typical savant cols if present
        for want in ["xwoba", "xslg", "hard_hit_percent", "barrel_batted_rate", "avg_exit_velocity", "xera", "k_percent", "bb_percent"]:
            if want in cols_lower:
                sav = sav.rename(columns={cols_lower[want]: want})
        for c in ["xwoba", "xslg", "hard_hit_percent", "barrel_batted_rate", "avg_exit_velocity", "xera", "k_percent", "bb_percent"]:
            if c in sav.columns:
                keep.append(c)
        sav = sav[keep].drop_duplicates("player_id_num")
        return df.merge(sav, left_on="pid_int", right_on="player_id_num", how="left")

    hitters = _join_savant(hitters, savant_bat_df)
    pitchers = _join_savant(pitchers, savant_pit_df)

    # Opportunity signals from recent reports
    opp = compute_opportunity_signals(recent_reports, lookback_days=14)

    def opp_count(name: str) -> int:
        return int(opp.get(name, {}).get("count", 0))

    def opp_notes(name: str) -> str:
        d = opp.get(name, {})
        notes = d.get("notes", []) or []
        conf = d.get("confidence", []) or []
        if not notes:
            return ""
        parts = []
        for i, n in enumerate(notes[:2]):
            c = conf[i] if i < len(conf) else ""
            if c:
                parts.append(f"[{c}] {n}")
            else:
                parts.append(n)
        return " | ".join(parts)

    def opp_best_conf(name: str) -> str:
        d = opp.get(name, {})
        conf = d.get("confidence", []) or []
        if "HIGH" in conf: return "HIGH"
        if "MEDIUM" in conf: return "MEDIUM"
        if conf: return conf[0]
        return ""

    # Score components
    def score_hitters(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["Add Score"] = []
            return df

        rank_num = pd.to_numeric(df.get("top500_rank"), errors="coerce")
        rank_pct = percentile_score(rank_num, higher_is_better=False).fillna(0)
        rank_score = 15 * rank_pct

        wrc = pd.to_numeric(df.get("wRC+"), errors="coerce")
        ops = pd.to_numeric(df.get("OPS"), errors="coerce")
        hr = pd.to_numeric(df.get("HR"), errors="coerce")
        sb = pd.to_numeric(df.get("SB"), errors="coerce")

        perf_score = 60 * (
            0.45 * percentile_score(wrc, True).fillna(0) +
            0.25 * percentile_score(ops, True).fillna(0) +
            0.20 * percentile_score(hr, True).fillna(0) +
            0.10 * percentile_score(sb, True).fillna(0)
        )

        sav_components = []
        for col in ["xwoba", "xslg", "hard_hit_percent", "barrel_batted_rate", "avg_exit_velocity"]:
            if col in df.columns:
                sav_components.append(percentile_score(df[col], True))
        sav_pct = pd.concat(sav_components, axis=1).mean(axis=1) if sav_components else pd.Series([0.0]*len(df), index=df.index)
        sav_score = 25 * sav_pct.fillna(0)

        df["Opportunity Count"] = df["Name"].apply(opp_count)
        df["Opportunity Notes"] = df["Name"].apply(opp_notes)
        df["Opportunity Confidence"] = df["Name"].apply(opp_best_conf)
        df["Opportunity Bonus"] = (df["Opportunity Count"].clip(upper=4) * 2).astype(float)  # 0..8
        df["Add Score"] = (rank_score + perf_score + sav_score + df["Opportunity Bonus"]).round(1)
        return df

    def score_pitchers(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["Add Score"] = []
            return df

        rank_num = pd.to_numeric(df.get("top500_rank"), errors="coerce")
        rank_pct = percentile_score(rank_num, higher_is_better=False).fillna(0)
        rank_score = 15 * rank_pct

        k_pct = pd.to_numeric(df.get("K%"), errors="coerce")
        bb_pct = pd.to_numeric(df.get("BB%"), errors="coerce")
        era = pd.to_numeric(df.get("ERA"), errors="coerce")
        fip = pd.to_numeric(df.get("FIP"), errors="coerce")
        sv = pd.to_numeric(df.get("SV"), errors="coerce")
        hld = pd.to_numeric(df.get("HLD"), errors="coerce")
        svh = (sv.fillna(0) + hld.fillna(0))

        perf_score = 60 * (
            0.30 * percentile_score(k_pct, True).fillna(0) +
            0.20 * percentile_score(bb_pct, False).fillna(0) +
            0.25 * percentile_score(era, False).fillna(0) +
            0.15 * percentile_score(fip, False).fillna(0) +
            0.10 * percentile_score(svh, True).fillna(0)
        )

        sav_components = []
        if "xera" in df.columns:
            sav_components.append(percentile_score(df["xera"], higher_is_better=False))
        if "hard_hit_percent" in df.columns:
            sav_components.append(percentile_score(df["hard_hit_percent"], higher_is_better=False))
        if "avg_exit_velocity" in df.columns:
            sav_components.append(percentile_score(df["avg_exit_velocity"], higher_is_better=False))
        if "k_percent" in df.columns:
            sav_components.append(percentile_score(df["k_percent"], higher_is_better=True))
        if "bb_percent" in df.columns:
            sav_components.append(percentile_score(df["bb_percent"], higher_is_better=False))

        sav_pct = pd.concat(sav_components, axis=1).mean(axis=1) if sav_components else pd.Series([0.0]*len(df), index=df.index)
        sav_score = 25 * sav_pct.fillna(0)

        df["Opportunity Count"] = df["Name"].apply(opp_count)
        df["Opportunity Notes"] = df["Name"].apply(opp_notes)
        df["Opportunity Confidence"] = df["Name"].apply(opp_best_conf)
        df["Opportunity Bonus"] = (df["Opportunity Count"].clip(upper=4) * 2).astype(float)  # 0..8
        df["Add Score"] = (rank_score + perf_score + sav_score + df["Opportunity Bonus"]).round(1)
        return df

    hitters = score_hitters(hitters)
    pitchers = score_pitchers(pitchers)

    scored = pd.concat([hitters, pitchers], ignore_index=True)
    scored["primary_pos"] = scored["Position"].apply(lambda x: _first_pos_for_sort(str(x)))
    scored["Savant"] = scored["pid_int"].apply(lambda x: button(baseball_savant_url(int(x)), "Savant", bg="#0b8043") if pd.notna(x) else "")

    # Positions-of-need guarantee
    used = set()
    forced_rows = []
    scored_sorted = scored.sort_values("Add Score", ascending=False)

    for pos in (positions_of_need or []):
        sub = scored_sorted[(scored_sorted["primary_pos"] == pos) & (~scored_sorted["Name"].isin(used))]
        if not sub.empty:
            r = sub.iloc[0]
            forced_rows.append(r)
            used.add(r["Name"])

    forced_df = pd.DataFrame(forced_rows) if forced_rows else pd.DataFrame(columns=scored_sorted.columns)
    rest = scored_sorted[~scored_sorted["Name"].isin(used)]
    final = pd.concat([forced_df, rest], ignore_index=True).head(10).copy()

    # Urgency
    final["Fills Need"] = final["primary_pos"].apply(lambda p: p in (positions_of_need or []))
    urg_list = []
    why_list = []
    for _, r in final.iterrows():
        urg, why = compute_waiver_urgency(
            add_score=safe_float(r.get("Add Score")),
            opp_count=int(r.get("Opportunity Count", 0) or 0),
            fills_need=bool(r.get("Fills Need")),
            confidence=str(r.get("Opportunity Confidence", "") or "")
        )
        urg_list.append(urg)
        why_list.append(why)
    final["Urgency"] = urg_list
    final["Urgency Why"] = why_list

    display = final[["Name", "Team", "Position", "Age", "top500_rank", "Add Score", "Urgency", "Opportunity Notes", "Savant"]].copy()
    display = display.rename(columns={"top500_rank": "Dynasty Rank"})
    display = display.dropna(subset=["Name"])
    return display

# =========================
# Prospect adds + urgency (tuned)
# =========================
def compute_prospect_adds(
    available_df: pd.DataFrame,
    dd_df: pd.DataFrame,
    bp_df: pd.DataFrame,
    recent_reports: list[dict],
    state: dict,
    year: int
) -> pd.DataFrame:
    if available_df is None or available_df.empty:
        return pd.DataFrame()

    cand = available_df.copy()
    if dd_df is not None and not dd_df.empty:
        cand = cand.merge(dd_df[["player_name", "dd_rank", "signed_year"]], on="player_name", how="left")
    if bp_df is not None and not bp_df.empty:
        cand = cand.merge(bp_df[["player_name", "bp_rank"]], on="player_name", how="left")

    name_to_id = {}
    for nm in cand["player_name"].tolist():
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid
    cand["pid"] = cand["player_name"].map(name_to_id)
    cand["pid_int"] = pd.to_numeric(cand.get("pid"), errors="coerce")
    cand["Level"] = cand["pid"].apply(lambda x: infer_player_level(int(x), year) if pd.notna(x) else "")

    # filters: asia
    cand = cand[~cand.apply(lambda r: looks_like_asia(str(r.get("team_abbrev","")), str(r.get("Level",""))), axis=1)].copy()

    # exclude current-year draftees (keep intl signings with evidence)
    def is_draft_pick(row):
        sy = row.get("signed_year", None)
        has_pro = bool(row.get("pid")) or bool(str(row.get("Level","")).strip())
        return exclude_current_year_draft_pick(sy, has_pro, year)

    cand = cand[~cand.apply(is_draft_pick, axis=1)].copy()

    # keep non-MLB only
    cand = cand[~cand["Level"].astype(str).str.upper().str.contains("MLB")].copy()
    if cand.empty:
        return pd.DataFrame()

    # Performance proxy (MiLB season)
    perf_raw = []
    for _, r in cand.iterrows():
        pid = r.get("pid")
        if pd.isna(pid) or pid is None:
            perf_raw.append(0.0)
            continue
        pid = int(pid)

        score = 0.0
        try:
            hit = statsapi.get("stats", {"group": "hitting", "stats": "season", "sportId": 21, "personIds": str(pid)})
            splits = hit.get("stats", [])[0].get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                hr = float(st.get("homeRuns", 0) or 0)
                sb = float(st.get("stolenBases", 0) or 0)
                obp = st.get("obp")
                avg = st.get("avg")
                score = hr*1.5 + sb*1.2
                try:
                    score += float(obp or 0) * 10
                except Exception:
                    pass
                try:
                    score += float(avg or 0) * 8
                except Exception:
                    pass
            else:
                pit = statsapi.get("stats", {"group": "pitching", "stats": "season", "sportId": 21, "personIds": str(pid)})
                ps = pit.get("stats", [])[0].get("splits", [])
                if ps:
                    st = ps[0].get("stat", {})
                    ip = st.get("inningsPitched")
                    era = st.get("era")
                    so = float(st.get("strikeOuts", 0) or 0)
                    bb = float(st.get("baseOnBalls", 0) or 0)
                    ipf = innings_to_float(ip) or 0.0
                    score = ipf*0.5 + so*0.25 - bb*0.1
                    try:
                        score += max(0.0, 8.0 - float(era)) * 2.5
                    except Exception:
                        pass
        except Exception:
            pass

        perf_raw.append(score)

    cand["perf_raw"] = perf_raw
    cand["perf_pct"] = percentile_score(cand["perf_raw"], True).fillna(0)

    cand["dd_rank_num"] = pd.to_numeric(cand.get("dd_rank"), errors="coerce")
    cand["bp_rank_num"] = pd.to_numeric(cand.get("bp_rank"), errors="coerce")
    dd_pct = percentile_score(cand["dd_rank_num"], higher_is_better=False).fillna(0)
    bp_pct = percentile_score(cand["bp_rank_num"], higher_is_better=False).fillna(0)

    # Buzz (7d)
    cutoff = now_utc() - timedelta(days=7)
    mentions = {}
    opp_hits = {}
    for it in recent_reports:
        try:
            utc = datetime.fromisoformat(it.get("utc","").replace("Z","+00:00"))
        except Exception:
            continue
        if utc < cutoff:
            continue
        p = it.get("player")
        title = it.get("title","") or ""
        if p:
            mentions[p] = mentions.get(p, 0) + 1
            if is_opportunity_text(title):
                opp_hits[p] = opp_hits.get(p, 0) + 1

    cand["mentions_7d"] = cand["player_name"].map(mentions).fillna(0).astype(int)
    cand["opp_7d"] = cand["player_name"].map(opp_hits).fillna(0).astype(int)
    buzz_pct = ((cand["mentions_7d"].clip(upper=5) + cand["opp_7d"].clip(upper=3)) / 8.0).fillna(0)

    # Score: 30/30/30/10
    cand["Add Score"] = (30*dd_pct + 30*bp_pct + 30*cand["perf_pct"] + 10*buzz_pct).round(1)

    # Prospect urgency: prioritize closeness (AAA/AA), buzz, opp hits
    def level_bonus(level: str) -> float:
        L = (level or "").upper()
        if "AAA" in L:
            return 1.2
        if "AA" in L:
            return 0.8
        if "A" in L:
            return 0.4
        return 0.2

    urg_vals = []
    for _, r in cand.iterrows():
        score = float(r.get("Add Score") or 0.0)
        pts = 0.0
        if score >= 80: pts += 2.7
        elif score >= 70: pts += 2.0
        elif score >= 60: pts += 1.3
        elif score >= 50: pts += 0.7
        else: pts += 0.3
        pts += level_bonus(r.get("Level",""))
        pts += min(1.8, int(r.get("opp_7d", 0) or 0) * 0.6)
        pts += min(1.0, int(r.get("mentions_7d", 0) or 0) * 0.15)

        if pts >= 5.4: urg = 5
        elif pts >= 4.3: urg = 4
        elif pts >= 3.2: urg = 3
        elif pts >= 2.2: urg = 2
        else: urg = 1
        urg_vals.append(urg)

    cand["Urgency"] = urg_vals

    out = cand.rename(columns={"player_name":"Name","team_abbrev":"Team","position":"Position","age":"Age"})
    out = out.sort_values("Add Score", ascending=False).head(10).copy()
    out["Savant"] = out["pid_int"].apply(lambda x: button(baseball_savant_url(int(x)), "Savant", bg="#0b8043") if pd.notna(x) else "")
    out = out[["Name","Team","Level","Age","Position","dd_rank","bp_rank","Add Score","Urgency","Savant","mentions_7d","opp_7d"]]
    out = out.rename(columns={"dd_rank":"Dynasty Dugout","bp_rank":"Baseball Prospectus","mentions_7d":"Mentions (7d)","opp_7d":"Opp Hits (7d)"})
    return out

# =========================
# Daily email builder
# =========================
def build_daily_bodies(official_items, starters, reports, opp_alerts, mlb_adds_df, roster_df, title_str):
    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    text = []
    text.append(f"Dynasty Daily Update — {title_str}\n")

    text.append("Transaction Wire (Official)")
    if official_items:
        for it in official_items:
            nm = it["player"]
            tm = team_by_player.get(nm, "")
            hdr = f"{nm} ({tm})" if tm else nm
            text.append(f"- {hdr}: {it['desc']}")
    else:
        text.append("No official transactions.")

    text.append("\nTonight's Starters")
    if starters:
        for s in starters:
            text.append(f"- {s['player']} {s['home_away']} {s['opponent']} — {s['first_pitch_ct']}")
    else:
        text.append("No rostered SPs listed as probable starters tonight (or probables not posted).")

    if opp_alerts:
        text.append("\nPlaying Time Opportunities")
        for a in opp_alerts:
            nm = a["player"]
            tm = team_by_player.get(nm, "")
            hdr = f"{nm} ({tm})" if tm else nm
            text.append(f"- [{a['confidence']}] {hdr}: {a['title']} ({a['source']}) {a['link']}".strip())

    if mlb_adds_df is not None and not mlb_adds_df.empty:
        text.append("\nMajor League Adds (Top 10)")
        for _, r in mlb_adds_df.iterrows():
            text.append(f"- Urgency {r.get('Urgency','')}: {r.get('Name','')} ({r.get('Team','')}) {r.get('Position','')} — Score {r.get('Add Score','')}")
    text.append("\nReports / Quotes")
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

    html.append(section_header("Tonight's Starters", "#1a73e8"))
    if starters:
        html.append("<ul style='margin:0; padding-left:18px;'>")
        for s in starters:
            html.append(f"<li style='margin:6px 0;'><b>{h(s['player'])}</b> {h(s['home_away'])} {h(s['opponent'])} — {h(s['first_pitch_ct'])}</li>")
        html.append("</ul>")
    else:
        html.append("<div style='color:#666;'>No rostered SPs listed as probable starters tonight (or probables not posted).</div>")

    if opp_alerts:
        html.append(section_header("Playing Time Opportunities", "#f9ab00"))
        for a in opp_alerts:
            tm = team_by_player.get(a["player"], "")
            hdr = f"{a['player']} ({tm})" if tm else a["player"]
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #fff1cc; background:#fff9e8; border-radius:12px;'>"
                f"<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                f"<div style='margin-top:6px;'><span style='font-weight:700;'>[{h(a['confidence'])}]</span> {h(a['title'])}</div>"
                f"<div style='margin-top:8px; display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                f"<span style='color:#555; font-size:13px;'>Source: {h(a['source'])}</span>"
                f"{button(a.get('link',''), 'News', bg='#5f6368')}"
                "</div></div>"
            )

    if mlb_adds_df is not None and not mlb_adds_df.empty:
        html.append(section_header("Major League Adds (Top 10)", "#0b8043"))
        html.append(render_table_html(mlb_adds_df, "Adds to Consider (Available)", html_cols={"Savant"}))

    html.append(section_header("Reports / Quotes", "#5f6368"))
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
                    f"{button(r['link'], 'News', bg='#5f6368')}"
                    "</div></div>"
                )
            html.append("</div>")
    else:
        html.append("<div style='color:#666;'>No matched reports.</div>")

    html.append("</body></html>")
    return text_body, "".join(html)

# =========================
# Daily runner
# =========================
def run_daily(lookback_hours=None):
    state = load_state()
    roster_df = load_roster()
    roster = roster_df["player_name"].tolist()
    year = local_now().year

    print(f"[roster] players={len(roster)} sample={roster[:10]}")

    if os.getenv("IS_SCHEDULED", "0") == "1":
        if not is_daily_time(state):
            print("[daily] Skipping - not 6am CT (or already sent).")
            save_state(state)
            return

    since = now_utc() - timedelta(hours=lookback_hours or 24)

    # Official transactions
    official_items = []
    for player in roster:
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        tx = tx_since(fetch_transactions(pid), since)
        for t in tx:
            official_items.append({"player": player, "desc": t["desc"], "utc": t["utc"]})
            append_jsonl(WEEKLY_OFFICIAL_PATH, {"player": player, **t})

    # News reports
    reports = fetch_reports(roster, state)
    for r in reports:
        append_jsonl(WEEKLY_REPORTS_PATH, r)

    starters = todays_starters_for_roster(roster_df)

    # Opportunity alerts for roster players (from reports + official)
    opp_reports = [x for x in reports if is_opportunity_text(x.get("title",""))]
    opp_alerts = []
    for r in opp_reports[:12]:
        opp_alerts.append({
            "player": r["player"],
            "confidence": opportunity_confidence(r.get("title","")),
            "title": r.get("title",""),
            "source": r.get("source",""),
            "link": r.get("link",""),
        })

    # MLB Adds section on Sun/Wed/Sat mornings (and also in manual runs if desired)
    mlb_adds_df = pd.DataFrame()
    include_adds = (os.getenv("IS_SCHEDULED", "0") == "1" and should_include_midweek_adds_now()) or (os.getenv("RUN_MODE","") == "daily_realnews_test")
    if include_adds:
        try:
            available_df = load_available_players()
            top500_df = load_top500_dynasty_rankings()
            sav_bat = fetch_savant_leaderboard(year, "batter", state)
            sav_pit = fetch_savant_leaderboard(year, "pitcher", state)
            positions_of_need = parse_positions_of_need_from_roster(roster_df)
            mlb_adds_df = compute_major_league_adds(available_df, top500_df, sav_bat, sav_pit, reports, state, year, positions_of_need)
        except Exception as e:
            print(f"[daily] MLB Adds error: {e}")
            mlb_adds_df = pd.DataFrame()

    print(f"[daily] official_items={len(official_items)} reports_items={len(reports)} starters={len(starters)} opp_alerts={len(opp_alerts)} adds_rows={len(mlb_adds_df) if mlb_adds_df is not None else 0}")

    # Quiet mode: nothing -> no email
    any_adds = mlb_adds_df is not None and not mlb_adds_df.empty
    if not official_items and not reports and not starters and not any_adds and not opp_alerts:
        if os.getenv("IS_SCHEDULED", "0") == "1":
            mark_daily_sent(state)
        save_state(state)
        return

    title_str = local_now().strftime("%b %d")
    text_body, html_body = build_daily_bodies(
        sorted(official_items, key=lambda x: x["utc"]),
        starters,
        sorted(reports, key=lambda x: (x["player"], x["source"], x["title"])),
        opp_alerts,
        mlb_adds_df,
        roster_df,
        title_str,
    )
    send_email(f"Dynasty Daily Update — {title_str}", text_body, html_body)

    if os.getenv("IS_SCHEDULED", "0") == "1":
        mark_daily_sent(state)

    save_state(state)

# =========================
# Weekly gates
# =========================
def should_send_weekly_now():
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7  # Monday 7am CT

def mark_weekly_sent(state):
    state["last_weekly_local_date"] = local_now().strftime("%Y-%m-%d")

# =========================
# Weekly runner (structure + merged stats tables)
# =========================
def run_weekly(force=False):
    state = load_state()
    now_local = local_now()
    year = now_local.year

    if os.getenv("IS_SCHEDULED","0") == "1" and not force:
        if not should_send_weekly_now():
            print("[weekly] Skipping - not Monday 7am CT.")
            save_state(state)
            return
        today = now_local.strftime("%Y-%m-%d")
        if state.get("last_weekly_local_date") == today:
            print("[weekly] Already sent today.")
            save_state(state)
            return

    roster_df = load_roster()
    roster_df["position"] = roster_df.get("position","").fillna("").astype(str)
    roster_names = roster_df["player_name"].tolist()

    # ids
    name_to_id = {}
    for nm in roster_names:
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid
    save_state(state)

    roster_pids = [int(name_to_id[nm]) for nm in roster_names if nm in name_to_id]

    # logs accumulated
    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = read_jsonl(WEEKLY_REPORTS_PATH)

    # windows
    w_start, w_end = previous_monday_sunday_window(now_local)

    # stats
    week_hit_mlb = fetch_statsapi_by_date_range("hitting", SPORT_ID_MLB, roster_pids, w_start, w_end)
    week_pit_mlb = fetch_statsapi_by_date_range("pitching", SPORT_ID_MLB, roster_pids, w_start, w_end)
    week_hit_milb = fetch_statsapi_by_date_range("hitting", SPORT_ID_MILB, roster_pids, w_start, w_end)
    week_pit_milb = fetch_statsapi_by_date_range("pitching", SPORT_ID_MILB, roster_pids, w_start, w_end)

    season_hit_mlb = fetch_statsapi_season("hitting", SPORT_ID_MLB, roster_pids)
    season_pit_mlb = fetch_statsapi_season("pitching", SPORT_ID_MLB, roster_pids)
    season_hit_milb = fetch_statsapi_season("hitting", SPORT_ID_MILB, roster_pids)
    season_pit_milb = fetch_statsapi_season("pitching", SPORT_ID_MILB, roster_pids)

    # FanGraphs overlays (MLB advanced)
    try:
        fg_hit = batting_stats(year)[["Name","Team","G","H","HR","RBI","SB","AVG","OBP","OPS","wRC+","K%","BB%"]].copy()
    except Exception:
        fg_hit = pd.DataFrame(columns=["Name"])

    try:
        fg_pit = pitching_stats(year)[["Name","Team","GS","IP","ERA","FIP","K%","BB%","SV","HLD"]].copy()
    except Exception:
        fg_pit = pd.DataFrame(columns=["Name"])

    fg_hit_map = {}
    if not fg_hit.empty:
        for _, r in fg_hit.iterrows():
            fg_hit_map[str(r.get("Name","")).strip()] = {k: r.get(k,"") for k in ["wRC+","K%","BB%","OPS"]}

    fg_pit_map = {}
    if not fg_pit.empty:
        for _, r in fg_pit.iterrows():
            fg_pit_map[str(r.get("Name","")).strip()] = {k: r.get(k,"") for k in ["FIP","K%","BB%"]}

    # roster info base
    roster_info = roster_df.copy()
    roster_info["pid"] = roster_info["player_name"].map(name_to_id)
    roster_info["Level"] = roster_info["pid"].apply(lambda x: infer_player_level(int(x), year) if pd.notna(x) and x else "")
    roster_info["is_pitcher"] = roster_info["position"].apply(is_pitcher_position)
    roster_info["is_mlb"] = roster_info["Level"].astype(str).str.upper().str.contains("MLB")

    # injury watch
    injury_players = set()
    injury_cards = []
    for it in official_news:
        if it.get("player") and is_injury_text(it.get("desc","")):
            injury_players.add(it["player"])
            injury_cards.append({"player": it["player"], "text": it.get("desc",""), "source": "Official", "link": ""})
    for it in reports_news:
        if it.get("player") and is_injury_text(it.get("title","")):
            injury_players.add(it["player"])
            injury_cards.append({"player": it["player"], "text": it.get("title",""), "source": it.get("source","Report"), "link": it.get("link","")})

    # opportunities (roster)
    opp_map = compute_opportunity_signals(reports_news, lookback_days=14)
    opp_alerts_weekly = []
    for p, d in opp_map.items():
        if p in roster_names and d.get("notes"):
            opp_alerts_weekly.append({
                "player": p,
                "confidence": "HIGH" if "HIGH" in (d.get("confidence") or []) else ("MEDIUM" if "MEDIUM" in (d.get("confidence") or []) else "LOW"),
                "notes": d.get("notes", [])[:2],
                "links": d.get("links", [])[:2],
            })
    opp_alerts_weekly.sort(key=lambda x: ({"HIGH":0,"MEDIUM":1,"LOW":2}.get(x["confidence"], 9), x["player"]))

    # two-start pitchers
    two_start_list = two_start_pitchers_week(roster_df)

    # hot week
    hot_hit_df, hot_sp_df, hot_rp_df = hot_week_tables(
        roster_info[["player_name","position"]],
        name_to_id,
        {**week_hit_mlb, **week_hit_milb},
        {**week_pit_mlb, **week_pit_milb},
    )

    # Build merged stats tables (weekly + season in one table each)
    def hitter_row(name, team, level, pos, pid, wk, ss, fg_adv=None):
        row = {
            "Player": name,
            "Team": team,
            "Level": level,
            "Position": pos,
            "W G": wk.get("gamesPlayed","") if wk else "",
            "W H": wk.get("hits","") if wk else "",
            "W HR": wk.get("homeRuns","") if wk else "",
            "W RBI": wk.get("rbi","") if wk else "",
            "W SB": wk.get("stolenBases","") if wk else "",
            "W AVG": wk.get("avg","") if wk else "",
            "W OBP": wk.get("obp","") if wk else "",
            "S G": ss.get("gamesPlayed","") if ss else "",
            "S H": ss.get("hits","") if ss else "",
            "S HR": ss.get("homeRuns","") if ss else "",
            "S RBI": ss.get("rbi","") if ss else "",
            "S SB": ss.get("stolenBases","") if ss else "",
            "S AVG": ss.get("avg","") if ss else "",
            "S OBP": ss.get("obp","") if ss else "",
            "OPS": (fg_adv or {}).get("OPS","") if fg_adv else "",
            "wRC+": (fg_adv or {}).get("wRC+","") if fg_adv else "",
            "K%": (fg_adv or {}).get("K%","") if fg_adv else "",
            "BB%": (fg_adv or {}).get("BB%","") if fg_adv else "",
            "Savant": button(baseball_savant_url(int(pid)), "Savant", bg="#0b8043") if pid else "",
        }
        return row

    hitters_rows = []
    for _, r in roster_info.iterrows():
        if r["is_pitcher"]:
            continue
        nm = r["player_name"]
        tm = r.get("team_abbrev","")
        pos = r.get("position","")
        pid = r.get("pid")
        level = "MLB" if r["is_mlb"] else r.get("Level","")

        if r["is_mlb"]:
            wk = week_hit_mlb.get(pid, {}) if pid else {}
            ss = season_hit_mlb.get(pid, {}) if pid else {}
            hitters_rows.append(hitter_row(nm, tm, "MLB", pos, pid, wk, ss, fg_hit_map.get(nm, {})))
        else:
            wk = week_hit_milb.get(pid, {}) if pid else {}
            ss = season_hit_milb.get(pid, {}) if pid else {}
            hitters_rows.append(hitter_row(nm, tm, level, pos, pid, wk, ss, None))

    hitters_df = pd.DataFrame(hitters_rows)
    if not hitters_df.empty:
        hitters_df["is_mlb"] = hitters_df["Level"].astype(str).str.upper().str.contains("MLB")
        hitters_df["pos_key"] = hitters_df["Position"].apply(_pos_sort_key_mlb)
        hitters_mlb = hitters_df[hitters_df["is_mlb"]].sort_values(["pos_key","Player"]).drop(columns=["is_mlb","pos_key"])
        hitters_milb = hitters_df[~hitters_df["is_mlb"]].sort_values(["Player"]).drop(columns=["is_mlb","pos_key"])
    else:
        hitters_mlb = pd.DataFrame()
        hitters_milb = pd.DataFrame()

    def pitcher_row(name, team, level, pos, pid, wk, ss, fg_adv=None, milb=False):
        row = {
            "Pitcher": name,
            "Team": team,
            "Level": level,
            "Position": pos,
            "W GS": wk.get("gamesStarted","") if wk else "",
            "W IP": wk.get("inningsPitched","") if wk else "",
            "W ERA": wk.get("era","") if wk else "",
            "W SO": wk.get("strikeOuts","") if wk else "",
            "W BB": wk.get("baseOnBalls","") if wk else "",
            "S GS": ss.get("gamesStarted","") if ss else "",
            "S IP": ss.get("inningsPitched","") if ss else "",
            "S ERA": ss.get("era","") if ss else "",
            "S SO": ss.get("strikeOuts","") if ss else "",
            "S BB": ss.get("baseOnBalls","") if ss else "",
            "FIP": (fg_adv or {}).get("FIP","") if fg_adv else "",
            "K%": (fg_adv or {}).get("K%","") if fg_adv else "",
            "BB%": (fg_adv or {}).get("BB%","") if fg_adv else "",
            "K/9": "",
            "BB/9": "",
            "Savant": button(baseball_savant_url(int(pid)), "Savant", bg="#0b8043") if pid else "",
        }
        if milb:
            ip = ss.get("inningsPitched","") if ss else ""
            so = ss.get("strikeOuts","") if ss else ""
            bb = ss.get("baseOnBalls","") if ss else ""
            k9v = k9(so, ip)
            bb9v = bb9(bb, ip)
            row["K/9"] = f"{k9v:.2f}" if k9v is not None else ""
            row["BB/9"] = f"{bb9v:.2f}" if bb9v is not None else ""
        return row

    pitchers_rows = []
    for _, r in roster_info.iterrows():
        if not r["is_pitcher"]:
            continue
        nm = r["player_name"]
        tm = r.get("team_abbrev","")
        pos = r.get("position","")
        pid = r.get("pid")
        level = "MLB" if r["is_mlb"] else r.get("Level","")

        if r["is_mlb"]:
            wk = week_pit_mlb.get(pid, {}) if pid else {}
            ss = season_pit_mlb.get(pid, {}) if pid else {}
            pitchers_rows.append(pitcher_row(nm, tm, "MLB", pos, pid, wk, ss, fg_pit_map.get(nm, {}), milb=False))
        else:
            wk = week_pit_milb.get(pid, {}) if pid else {}
            ss = season_pit_milb.get(pid, {}) if pid else {}
            pitchers_rows.append(pitcher_row(nm, tm, level, pos, pid, wk, ss, None, milb=True))

    pitchers_df = pd.DataFrame(pitchers_rows)
    if not pitchers_df.empty:
        pitchers_df["is_mlb"] = pitchers_df["Level"].astype(str).str.upper().str.contains("MLB")
        pit_mlb = pitchers_df[pitchers_df["is_mlb"]].sort_values(["Pitcher"]).drop(columns=["is_mlb"])
        pit_milb = pitchers_df[~pitchers_df["is_mlb"]].sort_values(["Pitcher"]).drop(columns=["is_mlb"])
    else:
        pit_mlb = pd.DataFrame()
        pit_milb = pd.DataFrame()

    # Adds sections at bottom
    positions_of_need = parse_positions_of_need_from_roster(roster_df)
    available_df = load_available_players()
    dd_df = load_dynasty_dugout_rankings()
    bp_df = load_baseball_prospectus_rankings()
    top500_df = load_top500_dynasty_rankings()
    sav_bat = fetch_savant_leaderboard(year, "batter", state)
    sav_pit = fetch_savant_leaderboard(year, "pitcher", state)

    mlb_adds_df = compute_major_league_adds(available_df, top500_df, sav_bat, sav_pit, reports_news, state, year, positions_of_need)
    prospect_adds_df = compute_prospect_adds(available_df, dd_df, bp_df, reports_news, state, year)

    # Major news (no injury duplicates)
    filtered_reports = [r for r in reports_news if not (r.get("player") in injury_players and is_injury_text(r.get("title","")))]

    # Build weekly email HTML
    subject = f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}"
    text_body = f"Dynasty Weekly Report ({w_start} to {w_end})\n(See HTML version for full formatting.)"

    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    html = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 8px 0;'>Dynasty Weekly Report — {h(now_local.strftime('%b %d, %Y'))}</h2>")
    html.append(f"<div style='color:#666; margin-bottom:6px;'>Stat window: <b>{h(w_start.strftime('%b %d'))} – {h(w_end.strftime('%b %d'))}</b></div>")

    # 1) Transaction Wire
    html.append(section_header("Transaction Wire (Official)", "#0b8043"))
    if not official_news:
        html.append("<div style='color:#666;'>No official transactions logged this week.</div>")
    else:
        byp = {}
        for it in official_news:
            byp.setdefault(it.get("player",""), []).append(it)
        for p in sorted([x for x in byp.keys() if x]):
            tm = team_by_player.get(p,"")
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt=''/>" if pid else ""
            tid = team_id_from_abbrev(tm, state)
            logo = f"<img src='{h(mlb_team_logo_url(tid))}' width='22' height='22' style='vertical-align:middle;' alt=''/>" if tid else ""
            hdr = f"{p} ({tm})" if tm else p

            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                f"<div style='margin-left:auto;'>{logo}</div>"
                "</div><ul style='margin:10px 0 0 0; padding-left:18px;'>"
            )
            for it in byp[p]:
                html.append(f"<li style='margin:6px 0;'>{h(it.get('desc',''))}</li>")
            html.append("</ul></div>")

    # 2) Injury Watch
    html.append(section_header("Injury Watch", "#d93025"))
    if not injury_cards:
        html.append("<div style='color:#666;'>No injuries detected in logged items this week.</div>")
    else:
        byp = {}
        for it in injury_cards:
            byp.setdefault(it["player"], []).append(it)
        for p in sorted(byp.keys()):
            tm = team_by_player.get(p,"")
            hdr = f"{p} ({tm})" if tm else p
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt=''/>" if pid else ""
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #ffe3e0; background:#fff5f4; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div></div>"
            )
            for it in byp[p]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #ffd3ce;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(it.get('text',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#6b2b22; font-size:13px;'>Source: {h(it.get('source',''))}</span>"
                    f"{button(it.get('link',''), 'News', bg='#d93025') if it.get('link') else ''}"
                    "</div></div>"
                )
            html.append("</div>")

    # 3) Two-start pitchers
    html.append(section_header("Two-Start Pitchers (Mon–Sun)", "#1a73e8"))
    if not two_start_list:
        html.append("<div style='color:#666;'>No 2-start probables detected (or probables not posted yet).</div>")
    else:
        html.append("<ul style='margin:0; padding-left:18px;'>")
        for t in two_start_list:
            det = "; ".join(t.get("details", [])[:6])
            html.append(f"<li style='margin:6px 0;'><b>{h(t['player'])}</b> — {h(str(t['starts']))} starts. <span style='color:#555'>{h(det)}</span></li>")
        html.append("</ul>")

    # 4) Hot week
    html.append(section_header("Hot Week Performances", "#f9ab00"))
    html.append("<div style='color:#666; margin-bottom:8px;'>Thresholds: HR ≥ 3, SB ≥ 4, OPS ≥ .950; SP: GS≥1 & ERA&lt;1.50; RP: SV+HLD ≥ 3.</div>")
    html.append(render_table_html(hot_hit_df, "Hot Hitters", html_cols=set()))
    html.append(render_table_html(hot_sp_df, "Hot Starters (GS≥1, ERA<1.50)", html_cols=set()))
    html.append(render_table_html(hot_rp_df, "Hot Relievers (SV+HLD≥3)", html_cols=set()))

    # 5) Weekly + Season stats (merged tables)
    html.append(section_header("Weekly and Season Stats", "#1a73e8"))

    if not hitters_mlb.empty:
        html.append(render_table_html(hitters_mlb, "MLB Hitters (sorted by position)", html_cols={"Savant"}))
    else:
        html.append("<div style='color:#666;'>No MLB hitters found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")

    if not hitters_milb.empty:
        html.append(render_table_html(hitters_milb, "Minor League Hitters (alphabetical)", html_cols={"Savant"}))
    else:
        html.append("<div style='color:#666;'>No minor-league hitters found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")

    if not pit_mlb.empty:
        html.append(render_table_html(pit_mlb, "MLB Pitchers", html_cols={"Savant"}))
    else:
        html.append("<div style='color:#666;'>No MLB pitchers found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")

    if not pit_milb.empty:
        html.append(render_table_html(pit_milb, "Minor League Pitchers", html_cols={"Savant"}))
    else:
        html.append("<div style='color:#666;'>No minor-league pitchers found.</div>")

    # 6) Major news (no injury repeats)
    html.append(section_header("Major News From The Week", "#5f6368"))
    if not filtered_reports:
        html.append("<div style='color:#666;'>No matched reports logged this week.</div>")
    else:
        byp = {}
        for r in sorted(filtered_reports, key=lambda x: (x.get("player",""), x.get("source",""), x.get("title",""))):
            byp.setdefault(r.get("player",""), []).append(r)
        for p in sorted([x for x in byp.keys() if x]):
            tm = team_by_player.get(p,"")
            hdr = f"{p} ({tm})" if tm else p
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt=''/>" if pid else ""
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div></div>"
            )
            for r in byp[p]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(r.get('title',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#555; font-size:13px;'>Source: {h(r.get('source',''))}</span>"
                    f"{button(r.get('link',''), 'News', bg='#5f6368')}"
                    "</div></div>"
                )
            html.append("</div>")

    # 7) Playing time opportunities (roster)
    html.append(section_header("Playing Time Opportunities", "#f9ab00"))
    if not opp_alerts_weekly:
        html.append("<div style='color:#666;'>No opportunity signals detected for rostered players in the past 14 days.</div>")
    else:
        for a in opp_alerts_weekly[:25]:
            tm = team_by_player.get(a["player"], "")
            hdr = f"{a['player']} ({tm})" if tm else a["player"]
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #fff1cc; background:#fff9e8; border-radius:12px;'>"
                f"<div style='font-size:16px;'><b>{h(hdr)}</b> <span style='color:#666; font-size:13px;'>({h(a['confidence'])})</span></div>"
            )
            for i, note in enumerate(a["notes"][:2]):
                link = a["links"][i] if i < len(a["links"]) else ""
                html.append(
                    "<div style='margin-top:10px; padding-top:10px; border-top:1px solid #f3e1aa;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(note)}</div>"
                    f"{button(link, 'News', bg='#5f6368') if link else ''}"
                    "</div>"
                )
            html.append("</div>")

    # 8) MLB Adds
    html.append(section_header("Major League Adds", "#0b8043"))
    if mlb_adds_df is None or mlb_adds_df.empty:
        html.append("<div style='color:#666;'>No MLB add candidates found in available pool (or MLB level couldn’t be detected).</div>")
    else:
        html.append(render_table_html(mlb_adds_df, "Top MLB Adds (Available) — max 10", html_cols={"Savant"}))

    # 9) Prospect Adds
    html.append(section_header("Prospect Adds", "#5f6368"))
    if prospect_adds_df is None or prospect_adds_df.empty:
        html.append("<div style='color:#666;'>No prospect add candidates found in available pool after filters.</div>")
    else:
        html.append(render_table_html(prospect_adds_df, "Top Prospect Adds (Available) — max 10", html_cols={"Savant"}))

    html.append("</body></html>")
    html_body = "".join(html)

    send_email(subject, text_body, html_body)

    if os.getenv("IS_SCHEDULED","0") == "1":
        mark_weekly_sent(state)
    save_state(state)

# =========================
# Tests / modes
# =========================
def run_smtp_test():
    send_email("SMTP Test", "If you received this, SMTP works.", "<b>If you received this, SMTP works.</b>")

def run_news_test():
    send_email("Daily Test", "Test email for daily.", "<b>Test email for daily.</b>")

def run_daily_realnews_test():
    run_daily(lookback_hours=24*14)

def run_weekly_test():
    run_weekly(force=True)

# =========================
# Main
# =========================
def main():
    ensure_state_files()
    mode = os.getenv("RUN_MODE", "daily").strip()
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
        raise SystemExit("Invalid RUN_MODE")

if __name__ == "__main__":
    main()
