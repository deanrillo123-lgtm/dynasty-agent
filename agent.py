
import os
import json
import smtplib
import re
import hashlib
import socket
from difflib import SequenceMatcher
from email.message import EmailMessage
from datetime import datetime, timedelta, date
from email.utils import parsedate_to_datetime
from dateutil import tz
import pytz
import pandas as pd
import requests
import feedparser
import statsapi
from pybaseball import batting_stats, pitching_stats
from bs4 import BeautifulSoup  # kept (even if unused) to match your environment

import tweepy
from typing import Optional, List, Dict, Tuple, Set, Any

print("[boot] dynasty agent file loaded", flush=True)

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

# Positive playing-time signals ONLY
POSITIVE_OPPORTUNITY_KEYWORDS = [
    "expected to start", "in line for", "more playing time", "bigger role", "everyday role",
    "role increase", "wins job", "takes over", "replacing", "fill in", "fill-in", "will see time",
    "gets time", "moving into rotation", "named the starter", "named starter", "named closer",
    "closing role", "in the rotation", "joins rotation", "called up", "call-up", "promoted",
    "batting leadoff", "batting second", "batting third", "in the lineup", "starting lineup"
]

NEGATIVE_OPPORTUNITY_KEYWORDS = [
    "injury", "injured", "il", "disabled list", "soreness", "rehab", "out for", "day-to-day",
    "optioned", "demoted", "sent down", "minors", "triple-a", "triple a", "dfa", "designated for assignment",
    "suspended", "suspension"
]

ASIA_HINTS = ["npb", "kbo", "cpbl", "japan", "korea", "taiwan", "nippon"]

MLB_HITTER_POS_ORDER = ["C", "1B", "2B", "SS", "3B", "OF", "DH"]

# IMPORTANT: prevent hangs from feedparser / sockets
socket.setdefaulttimeout(20)

# =========================
# Spring Training Config
# =========================
SPRING_GAME_TYPE = "S"  # StatsAPI gameType for Spring Training (preseason)
SPRING_TRAINING_MONTHS = {2, 3, 4}  # Feb–Apr (some games can spill early April)


# =========================
# Twitter/X Integration
# =========================
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "").strip()

TRACKED_TWITTER_ACCOUNTS = [
    "MLBPipeline", "PitcherList", "BaseballSavant", "FanGraphs",
    "BaseballProspectus", "louisanalysis", "prospects1500", "heckman_matt115",
    "pitcherlistplv", "baseballpro", "ibwaa", "brandondim",
    "jasonrrmartinez", "rotogut", "enosarris", "johnpgh",
    "downonthefarm", "ericcrossmlb", "prospectlarceny", "geoffpontesba",
    "the__arrival", "realjranderson", "mike_kurland", "tjstats",
    "thedynastyguru", "prospectslive", "imaginerybrickwall", "dynastytradeshq",
    "jeffzimmerman", "derecarty", "baseballamerica", "codifybaseball",
    "harryknowsball", "fanranked", "rotowire", "homerunapplesauce",
    "jasonrradawitz", "theprospectguy", "nathanpstrauss", "mlbplayeranalys",
    "dynastypicksups", "dynastyonestop", "sotop_23", "maxbay",
    "dynastybaseball", "batflipcrazy", "kylebland", "chrisblessing"
]

TWITTER_MIN_LIKES = 5
TWITTER_LOOKBACK_DAYS = 3


def test_twitter_bearer_token() -> Tuple[bool, str]:
    """Test if Twitter bearer token is valid"""
    if not TWITTER_BEARER_TOKEN:
        return False, "Bearer token not set in environment (TWITTER_BEARER_TOKEN)"
    
    try:
        client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
        user = client.get_me()
        return True, f"✅ Token valid! Authenticated as @{user.data.username}"
    except tweepy.errors.Unauthorized:
        return False, "❌ Bearer token is INVALID (401 Unauthorized)"
    except Exception as e:
        return False, f"❌ Error testing token: {str(e)}"


def fetch_tweets_about_players(
    player_names: List[str], 
    lookback_days: int = TWITTER_LOOKBACK_DAYS,
    exclude_cids: Set[str] = None
) -> List[Dict[str, Any]]:
    """Fetch tweets from tracked accounts mentioning roster players."""
    if not TWITTER_BEARER_TOKEN:
        log("[twitter] Bearer token not configured; skipping tweet fetch")
        return []
    
    if not player_names:
        return []
    
    exclude_cids = exclude_cids or set()
    
    try:
        client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
    except Exception as e:
        log(f"[twitter] Failed to initialize Tweepy client: {e}")
        return []
    
    tweets_found: List[Dict[str, Any]] = []
    cutoff = now_utc() - timedelta(days=lookback_days)
    accounts_query = " OR ".join([f"from:{acc}" for acc in TRACKED_TWITTER_ACCOUNTS])
    
    for player_name in player_names:
        try:
            query = f'"{player_name}" ({accounts_query}) -is:retweet lang:en'
            tweets = client.search_recent_tweets(
                query=query,
                max_results=10,
                tweet_fields=['created_at', 'public_metrics', 'author_id'],
                expansions=['author_id'],
                user_fields=['username', 'public_metrics'],
            )
            
            if not tweets.data:
                continue
            
            user_map = {}
            if tweets.includes and 'users' in tweets.includes:
                for user in tweets.includes['users']:
                    user_map[user.id] = user.username
            
            for tweet in tweets.data:
                cid = _content_id_stable(tweet.text, f"twitter/{tweet.id}")
                if cid in exclude_cids:
                    continue
                
                if tweet.created_at and tweet.created_at.replace(tzinfo=tz.tzutc()) < cutoff:
                    continue
                
                likes = tweet.public_metrics.get('like_count', 0) if tweet.public_metrics else 0
                if likes < TWITTER_MIN_LIKES:
                    continue
                
                author_username = user_map.get(tweet.author_id, "unknown")
                summary = _summarize_tweet(tweet.text, player_name)
                
                tweets_found.append({
                    "player": player_name,
                    "text": tweet.text,
                    "summary": summary,
                    "url": f"https://twitter.com/{author_username}/status/{tweet.id}",
                    "author": author_username,
                    "likes": likes,
                    "retweets": tweet.public_metrics.get('retweet_count', 0) if tweet.public_metrics else 0,
                    "created_at": tweet.created_at.isoformat() if tweet.created_at else "",
                })
        except Exception as e:
            log(f"[twitter] Error fetching tweets for {player_name}: {e}")
            continue
    
    tweets_found.sort(key=lambda x: x['likes'], reverse=True)
    log(f"[twitter] found {len(tweets_found)} tweets")
    return tweets_found


def _summarize_tweet(text: str, player_name: str) -> str:
    """Create one-sentence summary"""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(rf'\b{re.escape(player_name)}\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > 150:
        text = text[:147] + "..."
    return text if text else "Player mention"


def build_twitter_section_html(tweets: List[Dict[str, Any]]) -> str:
    """Build HTML section for tweets"""
    if not tweets:
        return ""
    
    html: List[str] = []
    html.append(section_header("Social Media Mentions (Twitter/X)", "#1DA1F2"))
    
    by_player: Dict[str, List[Dict[str, Any]]] = {}
    for tweet in tweets:
        player = tweet.get("player", "Unknown")
        by_player.setdefault(player, []).append(tweet)
    
    for player in sorted(by_player.keys()):
        player_tweets = by_player[player]
        html.append(
            "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e3f2fd; background:#f0f7ff; border-radius:12px;'>"
            f"<div style='font-size:16px; margin-bottom:8px;'><b>🐦 {h(player)}</b></div>"
        )
        
        for tweet in player_tweets[:3]:
            author = tweet.get("author", "unknown")
            likes = tweet.get("likes", 0)
            summary = tweet.get("summary", "Player mention")
            url = tweet.get("url", "")
            
            html.append(
                "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #b3e5fc;'>"
                f"<div style='margin:0 0 6px 0; color:#333; font-size:14px;'>{h(summary)}</div>"
                "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                f"<span style='color:#666; font-size:12px;'>@{h(author)} • {likes} ♥️</span>"
                f"{button(url, '𝕏 View', bg='#1DA1F2')}"
                "</div></div>"
            )
        html.append("</div>")
    
    return "".join(html)

# =========================
# Time helpers
# =========================
def local_now() -> datetime:
    return datetime.now(pytz.timezone(TZ_NAME))


def now_utc() -> datetime:
    return datetime.now(tz=tz.tzutc())


def _parse_iso_utc(utc_s: str) -> datetime:
    try:
        return datetime.fromisoformat(str(utc_s).replace("Z", "+00:00"))
    except Exception:
        return datetime(1970, 1, 1, tzinfo=tz.tzutc())


# =========================
# Startup / config helpers
# =========================
def log(msg: str) -> None:
    print(msg, flush=True)


def env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def validate_env(mode: str) -> None:
    required = {
        "EMAIL_ADDRESS": SENDER,
        "EMAIL_PASSWORD": SENDER_PW,
        "RECIPIENT_EMAIL": RECIPIENT,
    }

    if mode in {"daily", "weekly", "daily_realnews_test", "weekly_test", "spring_training_daily", "spring_daily_all"}:
        required["GSHEET_ID"] = GSHEET_ID
        required["ROSTER_GID"] = ROSTER_GID

    missing = [k for k, v in required.items() if not str(v).strip()]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def startup_summary(mode: str) -> None:
    redacted_sender = SENDER[:3] + "***" if SENDER else "<missing>"
    redacted_recipient = RECIPIENT[:3] + "***" if RECIPIENT else "<missing>"
    log(f"[startup] mode={mode}")
    log(f"[startup] sender={redacted_sender} recipient={redacted_recipient}")
    log(f"[startup] gsheet_id={'set' if GSHEET_ID else 'missing'} roster_gid={'set' if ROSTER_GID else 'missing'} available_gid={'set' if AVAILABLE_GID else 'missing'}")
    log(f"[startup] is_scheduled={os.getenv('IS_SCHEDULED', '0')} force_run={os.getenv('FORCE_RUN', '0')}")


# =========================
# State helpers
# =========================
def ensure_state_files() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(STATE_PATH):
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "player_cache": {},
                    "seen_rss": {},  # dict of {cid: utc_iso}
                    "last_daily_local_date": None,
                    "last_weekly_local_date": None,
                    "team_abbrev_map": {},
                    "cache_files": {},
                },
                f,
                indent=2,
            )

    for p in [WEEKLY_OFFICIAL_PATH, WEEKLY_REPORTS_PATH]:
        if not os.path.exists(p):
            with open(p, "w", encoding="utf-8") as f:
                f.write("")


def _stat_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def _stat_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _red_cross_html() -> str:
    return "<span style='color:#d93025;font-weight:900;'>✚</span>"


def _spectacular_hitter_week(wk: Dict[str, Any]) -> bool:
    ops = _stat_float(wk.get("ops"))
    hr = _stat_int(wk.get("homeRuns"))
    sb = _stat_int(wk.get("stolenBases"))
    avg = _stat_float(wk.get("avg"))
    ab = _stat_int(wk.get("atBats"))
    if ops is not None and ops >= 1.050:
        return True
    if hr is not None and hr >= 3:
        return True
    if sb is not None and sb >= 4:
        return True
    if (avg is not None and avg >= 0.400) and (ab is not None and ab >= 10):
        return True
    return False


def _cold_hitter_week(wk: Dict[str, Any]) -> bool:
    ops = _stat_float(wk.get("ops"))
    ab = _stat_int(wk.get("atBats"))
    if ops is None:
        return False
    if ab is None or ab < 10:
        return False
    return ops <= 0.500


def innings_to_float(ip: Any) -> Optional[float]:
    if ip is None or ip == "":
        return None
    s = str(ip)
    if "." not in s:
        try:
            return float(s)
        except Exception:
            return None
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
        return whole + 1 / 3
    if frac == 2:
        return whole + 2 / 3
    return whole


def _spectacular_pitcher_week(wk: Dict[str, Any]) -> bool:
    gs = _stat_int(wk.get("gamesStarted"))
    era = _stat_float(wk.get("era"))
    so = _stat_int(wk.get("strikeOuts"))
    ipf = innings_to_float(wk.get("inningsPitched"))
    sv = _stat_int(wk.get("saves"))
    hld = _stat_int(wk.get("holds"))

    if gs is not None and gs >= 1:
        if era is not None and era <= 1.00 and so is not None and so >= 10 and ipf is not None and ipf >= 5.0:
            return True
        return False

    total = (sv or 0) + (hld or 0)
    if total >= 3 and era is not None and era <= 1.00 and ipf is not None and ipf >= 3.0:
        return True
    return False


def _cold_pitcher_week(wk: Dict[str, Any]) -> bool:
    era = _stat_float(wk.get("era"))
    ipf = innings_to_float(wk.get("inningsPitched"))
    if era is None or ipf is None:
        return False
    return (era >= 7.00) and (ipf >= 4.0)


def build_status_html(player_name: str, injury_players: Set[str], wk: Optional[Dict[str, Any]], is_pitcher: bool) -> str:
    parts: List[str] = []
    if player_name in injury_players:
        parts.append(_red_cross_html())

    if is_pitcher:
        if _spectacular_pitcher_week(wk or {}):
            parts.append("🔥")
        elif _cold_pitcher_week(wk or {}):
            parts.append("🧊")
    else:
        if _spectacular_hitter_week(wk or {}):
            parts.append("🔥")
        elif _cold_hitter_week(wk or {}):
            parts.append("🧊")

    return "".join(parts)


def load_state() -> Dict[str, Any]:
    ensure_state_files()
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        st = json.load(f)

    # MIGRATION: old list format -> dict format
    if "seen_rss" not in st:
        st["seen_rss"] = {}
    if "seen_rss_ids" in st and isinstance(st["seen_rss_ids"], list):
        nowi = now_utc().isoformat()
        for cid in st["seen_rss_ids"]:
            st["seen_rss"][cid] = nowi
        st.pop("seen_rss_ids", None)

    return st


def save_state(state: Dict[str, Any]) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def append_jsonl(path: str, record: Dict[str, Any]) -> None:
    ensure_state_files()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    ensure_state_files()
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
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
def send_email(subject: str, text_body: str, html_body: str) -> None:
    if not (SENDER and SENDER_PW and RECIPIENT):
        raise RuntimeError("Missing EMAIL_ADDRESS / EMAIL_PASSWORD / RECIPIENT_EMAIL.")

    log(f"[email] preparing subject={subject!r} to={RECIPIENT}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Dynasty Agent <{SENDER}>"
    msg["To"] = RECIPIENT

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        log(f"[email] connecting host={SMTP_HOST} port={SMTP_PORT}")
        server.starttls()
        log("[email] starttls ok")
        server.login(SENDER, SENDER_PW)
        log("[email] login ok")
        server.send_message(msg)
        log("[email] send_message ok")


# =========================
# HTML helpers
# =========================
def h(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
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


def render_table_html(df: pd.DataFrame, title: str, html_cols: Optional[Set[str]] = None) -> str:
    """
    Supports:
    - grouped week/season stat headers
    - non-group columns before week stats
    - non-group columns between week and season stats (e.g. Status)
    - non-group columns after season stats (e.g. Savant)
    """
    html_cols = set(html_cols or [])
    if df is None or df.empty:
        return f"<h4 style='margin:14px 0 6px 0;'>{h(title)}</h4><div style='color:#666;'>No data.</div>"

    cols = list(df.columns)
    rows = df.fillna("").astype(str).values.tolist()

    def is_w(c: Any) -> bool:
        return isinstance(c, str) and c.startswith("W ")

    def is_s(c: Any) -> bool:
        return isinstance(c, str) and c.startswith("S ")

    week_idxs = [i for i, c in enumerate(cols) if is_w(c)]
    season_idxs = [i for i, c in enumerate(cols) if is_s(c)]
    week_cols = [cols[i] for i in week_idxs]
    season_cols = [cols[i] for i in season_idxs]
    has_groups = bool(week_cols or season_cols)

    year_label = f"{local_now().year} Stats"
    week_label = "Last Week's Stats"
    divider_css = "border-left:4px solid #111;"

    first_season_idx = season_idxs[0] if season_idxs else None

    pre_cols: List[str] = []
    mid_cols: List[str] = []
    post_cols: List[str] = []

    if has_groups:
        all_group_idxs = sorted(week_idxs + season_idxs)
        first_group_idx = min(all_group_idxs)
        last_group_idx = max(all_group_idxs)
        last_week_idx = max(week_idxs) if week_idxs else None
        first_season_group_idx = min(season_idxs) if season_idxs else None

        pre_cols = [c for i, c in enumerate(cols) if i < first_group_idx and not (is_w(c) or is_s(c))]

        if last_week_idx is not None and first_season_group_idx is not None:
            mid_cols = [
                c
                for i, c in enumerate(cols)
                if last_week_idx < i < first_season_group_idx and not (is_w(c) or is_s(c))
            ]

        post_cols = [c for i, c in enumerate(cols) if i > last_group_idx and not (is_w(c) or is_s(c))]
    else:
        pre_cols = cols[:]

    out: List[str] = []
    out.append(f"<h4 style='margin:16px 0 8px 0; text-align:center;'>{h(title)}</h4>")
    out.append("<div style='overflow-x:auto; border:1px solid #e8e8e8; border-radius:10px;'>")
    out.append("<table style='border-collapse:collapse; width:100%; font-size:12.5px;'>")
    out.append("<thead>")

    if has_groups:
        out.append("<tr style='background:#f6f7f9;'>")

        for c in pre_cols:
            out.append(
                "<th rowspan='2' style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
                f"{h(str(c))}</th>"
            )

        if week_cols:
            out.append(
                f"<th colspan='{len(week_cols)}' style='text-align:center; padding:8px 10px; "
                f"border-bottom:1px solid #e8e8e8; white-space:nowrap;'>{h(week_label)}</th>"
            )

        for c in mid_cols:
            out.append(
                "<th rowspan='2' style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
                f"{h(str(c))}</th>"
            )

        if season_cols:
            left_border = divider_css if week_cols or mid_cols else ""
            out.append(
                f"<th colspan='{len(season_cols)}' style='text-align:center; padding:8px 10px; "
                f"border-bottom:1px solid #e8e8e8; white-space:nowrap; {left_border}'>{h(year_label)}</th>"
            )

        for c in post_cols:
            out.append(
                "<th rowspan='2' style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
                f"{h(str(c))}</th>"
            )

        out.append("</tr>")

        out.append("<tr style='background:#f6f7f9;'>")

        for c in week_cols:
            label = str(c)[2:]
            out.append(
                "<th style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
                f"{h(label)}</th>"
            )

        for j, c in enumerate(season_cols):
            label = str(c)[2:]
            lb = divider_css if j == 0 else ""
            out.append(
                f"<th style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap; {lb}'>"
                f"{h(label)}</th>"
            )

        out.append("</tr>")
    else:
        out.append("<tr style='background:#f6f7f9;'>")
        for c in cols:
            out.append(
                "<th style='text-align:center; padding:8px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap;'>"
                f"{h(str(c))}</th>"
            )
        out.append("</tr>")

    out.append("</thead>")
    out.append("<tbody>")

    for i, r in enumerate(rows):
        bg = "#ffffff" if i % 2 == 0 else "#fbfbfc"
        out.append(f"<tr style='background:{bg};'>")
        for idx, (c, cell) in enumerate(zip(cols, r)):
            cell_html = cell if c in html_cols else h(cell)
            lb = divider_css if (first_season_idx is not None and idx == first_season_idx) else ""
            out.append(
                f"<td style='padding:7px 10px; border-bottom:1px solid #f0f0f0; white-space:nowrap; {lb}'>"
                f"{cell_html}</td>"
            )
        out.append("</tr>")

    out.append("</tbody></table></div>")
    return "".join(out)


# =========================
# URLs
# =========================
def mlb_headshot_url(mlbam_id: int) -> str:
    return "https://content.mlb.com/images/headshots/current/60x60/%s.png" % mlbam_id


def mlb_team_logo_url(team_id: int) -> str:
    return "https://www.mlbstatic.com/team-logos/%s.png" % team_id


def baseball_savant_url(mlbam_id: int) -> str:
    return "https://baseballsavant.mlb.com/savant-player/%s" % mlbam_id


def baseball_reference_search_url(player_name: str) -> str:
    from urllib.parse import quote_plus
    return f"https://www.baseball-reference.com/search/search.fcgi?search={quote_plus(player_name or '')}"


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


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
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


def parse_positions_of_need_from_roster(roster_df: pd.DataFrame) -> List[str]:
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

    cleaned: List[str] = []
    for p in raw:
        p2 = p.strip().upper()
        if not p2:
            continue
        p2 = p2.replace("LF", "OF").replace("CF", "OF").replace("RF", "OF")
        cleaned.append(p2)

    out: List[str] = []
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

    out_rows: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(player_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        out_rows.append(
            {
                "player_name": name,
                "team_abbrev": str(row.get(team_col, "")).strip() if team_col else "",
                "position": str(row.get(pos_col, "")).strip() if pos_col else "",
                "age": str(row.get(age_col, "")).strip() if age_col else "",
            }
        )

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

    rows: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(player_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rows.append(
            {
                "player_name": name,
                "team_abbrev": str(row.get(team_col, "")).strip() if team_col else "",
                "position": str(row.get(pos_col, "")).strip() if pos_col else "",
                "age": str(row.get(age_col, "")).strip() if age_col else "",
            }
        )
    return pd.DataFrame(rows).drop_duplicates(subset=["player_name"]).reset_index(drop=True)


def load_dynasty_dugout_rankings() -> pd.DataFrame:
    df = read_sheet_tab_csv(GSHEET_ID, DD_RANK_GID)
    name_col = _pick_col(df, ["player", "player_name", "name", "player name"])
    rank_col = _pick_col(df, ["rank", "ranking", "dd_rank"])
    signed_col = _pick_col(df, ["signed", "signed_year", "signed year", "signed/drafted"])

    if not name_col or not rank_col:
        return pd.DataFrame(columns=["player_name", "dd_rank", "signed_year"])

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        name = _norm_name(str(row.get(name_col, "")))
        if not name or name.lower() in ("player", "name"):
            continue
        rk_raw = str(row.get(rank_col, "")).strip()
        try:
            rk = int(float(rk_raw))
        except Exception:
            continue

        signed_year: Optional[int] = None
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

    rows: List[Dict[str, Any]] = []
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

    rows: List[Dict[str, Any]] = []
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
def scrub_bad_player_cache(state: Dict[str, Any]) -> int:
    cache = state.get("player_cache", {}) or {}
    bad_keys: List[str] = []
    for k, v in cache.items():
        if isinstance(v, dict):
            bad_keys.append(k)
        else:
            try:
                _ = int(v)
            except Exception:
                bad_keys.append(k)
    for k in bad_keys:
        cache.pop(k, None)
    state["player_cache"] = cache
    return len(bad_keys)


def lookup_mlbam_id(player_name: str, state: Dict[str, Any]) -> Optional[int]:
    cache = state.get("player_cache", {}) or {}

    if player_name in cache:
        v = cache[player_name]
        if isinstance(v, dict):
            for k in ("id", "personId", "mlbam_id"):
                if k in v:
                    try:
                        pid = int(v[k])
                        cache[player_name] = pid
                        state["player_cache"] = cache
                        return pid
                    except Exception:
                        pass
            cache.pop(player_name, None)
            state["player_cache"] = cache
        else:
            try:
                return int(v)
            except Exception:
                cache.pop(player_name, None)
                state["player_cache"] = cache

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


def build_team_abbrev_map(state: Dict[str, Any]) -> Dict[str, int]:
    cache = state.get("team_abbrev_map")
    if isinstance(cache, dict) and cache:
        return cache
    mapping: Dict[str, int] = {}
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


def team_id_from_abbrev(team_abbrev: str, state: Dict[str, Any]) -> Optional[int]:
    ab = (team_abbrev or "").strip().upper()
    if not ab:
        return None
    mapping = build_team_abbrev_map(state)
    return mapping.get(ab)


# =========================
# Transactions
# =========================
def fetch_transactions(pid: int) -> List[Dict[str, Any]]:
    try:
        data = statsapi.get("person", {"personId": pid, "hydrate": "transactions"})
        people = data.get("people", [])
        if not people:
            return []
        return people[0].get("transactions", [])
    except Exception:
        return []


def tx_since(tx_list: List[Dict[str, Any]], since: datetime) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
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
def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _content_id_stable(title: str, link: str) -> str:
    return hashlib.sha1(f"{title}|{link}".encode("utf-8", errors="ignore")).hexdigest()


def _build_name_patterns(names: List[str]):
    return [(name, re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)) for name in names]


def _google_news_url(query: str) -> str:
    from urllib.parse import quote_plus
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


def _build_google_news_sources(names: List[str]) -> List[Dict[str, str]]:
    sources: List[Dict[str, str]] = []
    chunk_size = 8
    for i in range(0, len(names), chunk_size):
        chunk = names[i:i + chunk_size]
        or_part = " OR ".join([f"\"{n}\"" for n in chunk])
        query = f"({or_part}) baseball (injury OR soreness OR IL OR optioned OR promoted OR demoted OR trade OR traded OR DFA OR rehab OR suspension OR role)"
        sources.append({"name": f"Google News (Roster {i // chunk_size + 1})", "url": _google_news_url(query)})

    sources.append({"name": "Google News (CBS Fantasy)", "url": _google_news_url("site:cbssports.com/fantasy baseball")})
    sources.append({"name": "Google News (RotoBaller)", "url": _google_news_url("site:rotoballer.com baseball")})
    sources.append({"name": "Google News (Pitcher List)", "url": _google_news_url("site:pitcherlist.com baseball")})
    sources.append({"name": "Google News (@pitcherlistplv)", "url": _google_news_url('"pitcherlistplv"')})
    return sources


def _prune_seen_rss(state: Dict[str, Any], keep_days: int = 35) -> None:
    seen = state.get("seen_rss", {}) or {}
    cutoff = now_utc() - timedelta(days=keep_days)
    new_seen: Dict[str, str] = {}
    for cid, utc_s in seen.items():
        try:
            dt = datetime.fromisoformat(str(utc_s).replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= cutoff:
            new_seen[cid] = dt.isoformat()
    state["seen_rss"] = new_seen


def _headline_event_bucket(title: str) -> str:
    t = (title or "").lower()

    buckets = [
        ("injury", ["injury", "injured", "il", "injured list", "soreness", "strain", "sprain", "mri", "rehab", "shut down", "day-to-day"]),
        ("promotion", ["called up", "call-up", "promoted", "promotion", "recalled"]),
        ("demotion", ["optioned", "demoted", "sent down"]),
        ("transaction", ["dfa", "designated for assignment", "traded", "trade", "claimed", "released", "signed"]),
        ("role", ["named starter", "named the starter", "named closer", "closing role", "moving into rotation", "in the rotation",
                  "batting leadoff", "batting second", "batting third", "everyday role", "more playing time",
                  "bigger role", "role increase"]),
        ("return", ["activated", "returns", "returning", "reinstated"]),
    ]

    for bucket, words in buckets:
        if any(w in t for w in words):
            return bucket
    return "news"


def _normalize_headline_core(title: str, player_name: str) -> str:
    t = (title or "").lower()
    p = (player_name or "").lower()

    if p:
        t = re.sub(rf"\b{re.escape(p)}\b", " ", t, flags=re.IGNORECASE)

    t = re.sub(r"\b(mlb|milb|report|reports|reportedly|source|sources|says|say|update|updates)\b", " ", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    words = [w for w in t.split() if len(w) > 2]
    return " ".join(words[:8])


def _semantic_story_key(player_name: str, title: str) -> str:
    bucket = _headline_event_bucket(title)
    core = _normalize_headline_core(title, player_name)
    return f"{player_name.lower()}|{bucket}|{core}"


def _source_priority(source_name: str) -> int:
    s = (source_name or "").lower()
    if "mlb.com" in s:
        return 100
    if "mlb pipeline" in s:
        return 95
    if "milb.com" in s:
        return 92
    if "baseball america" in s:
        return 90
    if "fangraphs" in s:
        return 88
    if "baseball prospectus" in s:
        return 86
    if "pitcher list" in s:
        return 84
    if "mlbtr" in s:
        return 82
    if "cbs" in s:
        return 78
    if "google news" in s:
        return 70
    return 60


def _headline_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()


def _same_story_for_player(player_name: str, title_a: str, title_b: str) -> bool:
    ka = _semantic_story_key(player_name, title_a)
    kb = _semantic_story_key(player_name, title_b)
    if ka == kb:
        return True

    if _headline_event_bucket(title_a) != _headline_event_bucket(title_b):
        return False

    core_a = _normalize_headline_core(title_a, player_name)
    core_b = _normalize_headline_core(title_b, player_name)

    if core_a and core_b and (core_a in core_b or core_b in core_a):
        return True

    if core_a and core_b and _headline_similarity(core_a, core_b) >= 0.86:
        return True

    if _headline_similarity(title_a, title_b) >= 0.90:
        return True

    return False


def _story_specificity_score(player_name: str, title: str) -> int:
    core = _normalize_headline_core(title, player_name)
    if not core:
        return 0
    return len(set(core.split()))


def _story_rank(item: Dict[str, Any]) -> Tuple[int, int, int, float]:
    player = item.get("player", "") or ""
    title = item.get("title", "") or item.get("desc", "") or ""
    source = item.get("source", "") or ""
    dt = _parse_iso_utc(item.get("utc", "")).timestamp()
    return (
        _source_priority(source),
        _story_specificity_score(player, title),
        len(title),
        dt,
    )


def dedupe_reports_semantic(items: List[Dict[str, Any]], within_days: int = 5) -> List[Dict[str, Any]]:
    if not items:
        return []

    kept: List[Dict[str, Any]] = []
    items_sorted = sorted(items, key=_story_rank, reverse=True)

    for item in items_sorted:
        player = item.get("player", "") or ""
        title = item.get("title", "") or item.get("desc", "") or ""
        if not player or not title:
            kept.append(item)
            continue

        item_dt = _parse_iso_utc(item.get("utc", ""))

        dup_idx: Optional[int] = None
        for i, existing in enumerate(kept):
            if existing.get("player", "") != player:
                continue

            ex_title = existing.get("title", "") or existing.get("desc", "") or ""
            if not ex_title:
                continue

            ex_dt = _parse_iso_utc(existing.get("utc", ""))
            if abs((item_dt - ex_dt).total_seconds()) > within_days * 86400:
                continue

            if _same_story_for_player(player, title, ex_title):
                dup_idx = i
                break

        if dup_idx is None:
            kept.append(item)
        else:
            if _story_rank(item) > _story_rank(kept[dup_idx]):
                kept[dup_idx] = item

    return sorted(kept, key=lambda x: (_parse_iso_utc(x.get("utc", "")).timestamp(), x.get("player", ""), x.get("source", "")))


def _parse_feed_entry_datetime(entry: Any) -> Optional[datetime]:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return datetime(*entry.published_parsed[:6]).replace(tzinfo=tz.tzutc())
    except Exception:
        pass
    try:
        if hasattr(entry, "updated_parsed") and entry.updated_parsed:
            return datetime(*entry.updated_parsed[:6]).replace(tzinfo=tz.tzutc())
    except Exception:
        pass
    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(tz.tzutc())
            except Exception:
                pass
    return None


def fetch_reports(names: List[str], state: Dict[str, Any], max_age_days: int = 7) -> List[Dict[str, Any]]:
    sources: List[Dict[str, str]] = [
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

    _prune_seen_rss(state, keep_days=35)
    seen = state.get("seen_rss", {}) or {}
    patterns = _build_name_patterns(names)
    matched: List[Dict[str, Any]] = []

    cutoff = now_utc() - timedelta(days=max_age_days)

    for src in sources:
        try:
            resp = requests.get(src["url"], timeout=20, headers={"User-Agent": "dynasty-agent/1.0"})
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            entries = getattr(feed, "entries", [])[:150]
        except Exception:
            continue

        for e in entries:
            title = _normalize(getattr(e, "title", ""))
            link = _normalize(getattr(e, "link", "")) or _normalize(getattr(e, "id", ""))
            summary = _normalize(getattr(e, "summary", "")) if hasattr(e, "summary") else ""
            blob = f"{title} {summary}"

            if not title or not link:
                continue

            pub_dt = _parse_feed_entry_datetime(e) or now_utc()
            if pub_dt < cutoff:
                continue

            title_l = title.lower()
            link_l = link.lower()
            bad_title_phrases = [
                "stats, age, position",
                "fantasy & news",
                "player page",
                "roster",
                "depth chart",
            ]
            if any(p in title_l for p in bad_title_phrases):
                continue
            if "/player/" in link_l and ("news" not in link_l) and ("article" not in link_l):
                continue

            cid = _content_id_stable(title, link)
            if cid in seen:
                continue

            players: List[str] = []
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

            seen[cid] = pub_dt.isoformat()

    state["seen_rss"] = seen
    return dedupe_reports_semantic(matched)


# =========================
# Opportunity & injury detection
# =========================
def is_injury_text(s: str) -> bool:
    t = (s or "").lower()
    return any(k in t for k in INJURY_KEYWORDS)


def is_positive_opportunity_text(s: str) -> bool:
    t = (s or "").lower()
    if any(k in t for k in NEGATIVE_OPPORTUNITY_KEYWORDS):
        return False
    return any(k in t for k in POSITIVE_OPPORTUNITY_KEYWORDS)


def opportunity_confidence(title: str) -> str:
    t = (title or "").lower()
    high = [
        "named the starter", "named starter", "named closer", "everyday role", "moving into rotation",
        "called up", "promoted", "wins job", "takes over"
    ]
    if any(x in t for x in high):
        return "HIGH"
    med = [
        "expected to start", "in line for", "more playing time", "bigger role",
        "role increase", "fill in", "replacing", "in the lineup"
    ]
    if any(x in t for x in med):
        return "MEDIUM"
    return "LOW"


def summarize_opportunity_net(title: str) -> str:
    t = (title or "").strip()
    tl = t.lower()
    if "called up" in tl or "call-up" in tl or "promoted" in tl:
        return "Net: called up / promoted → playing time bump likely."
    if "named closer" in tl or "closing role" in tl:
        return "Net: moved into closing mix → save chances up."
    if "moving into rotation" in tl or "joins rotation" in tl or "in the rotation" in tl or "named the starter" in tl or "named starter" in tl:
        return "Net: rotation role → starts/innings up."
    if "everyday role" in tl:
        return "Net: everyday role → steady PA volume."
    if "expected to start" in tl or "in the lineup" in tl or "starting lineup" in tl:
        return "Net: starting usage → near-term PA/innings up."
    if "batting leadoff" in tl:
        return "Net: leadoff spot → PA + runs upside."
    if "batting second" in tl or "batting third" in tl:
        return "Net: premium lineup slot → PA + RBI/R upside."
    if "more playing time" in tl or "bigger role" in tl or "role increase" in tl:
        return "Net: role increased → usage trending up."
    return f"Net: {t}"


def compute_opportunity_signals(items: List[Dict[str, Any]], lookback_days: int = 14) -> Dict[str, Dict[str, Any]]:
    items = dedupe_reports_semantic(items)
    cutoff = now_utc() - timedelta(days=lookback_days)
    out: Dict[str, Dict[str, Any]] = {}
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

        if is_positive_opportunity_text(title):
            d = out.setdefault(player, {"count": 0, "notes": [], "links": [], "confidence": []})
            d["count"] += 1
            if len(d["notes"]) < 3:
                d["notes"].append(summarize_opportunity_net(title))
                d["links"].append(link)
                d["confidence"].append(opportunity_confidence(title))
    return out


# =========================
# Daily gate & starters
# =========================
def is_daily_time(state: Dict[str, Any]) -> Tuple[bool, str]:
    ln = local_now()
    wd = ln.weekday()  # Mon=0 ... Sun=6

    if wd in (2, 5, 6):  # Wed, Sat, Sun
        if not (ln.hour == 6 and ln.minute >= 30):
            return False, f"scheduled window mismatch: now={ln.isoformat()} expected Wed/Sat/Sun at 6:30-6:59 {TZ_NAME}"
    else:
        if ln.hour != 6:
            return False, f"scheduled window mismatch: now={ln.isoformat()} expected 6:00-6:59 {TZ_NAME}"

    today = ln.strftime("%Y-%m-%d")
    if state.get("last_daily_local_date") == today:
        return False, f"already sent today: {today}"

    return True, "ok"


def mark_daily_sent(state: Dict[str, Any]) -> None:
    state["last_daily_local_date"] = local_now().strftime("%Y-%m-%d")


def should_include_midweek_adds_now() -> bool:
    ln = local_now()
    wd = ln.weekday()
    if wd in (2, 5, 6):
        return ln.hour == 6 and ln.minute >= 30
    else:
        return ln.hour == 6


def _fmt_local_time_safe(dt: datetime) -> str:
    return dt.strftime("%I:%M %p %Z").lstrip("0")


def todays_starters_for_roster(roster_df: pd.DataFrame) -> List[Dict[str, str]]:
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

    out: List[Dict[str, str]] = []
    for g in games:
        game_dt_utc = g.get("game_datetime")
        first_pitch_ct = ""
        if game_dt_utc:
            try:
                dt = datetime.fromisoformat(game_dt_utc.replace("Z", "+00:00"))
                ct = dt.astimezone(pytz.timezone(TZ_NAME))
                first_pitch_ct = _fmt_local_time_safe(ct)
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

        def any_played(stats_blob: Dict[str, Any]) -> bool:
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
    return any(hint in blob for hint in ASIA_HINTS)


# =========================
# Savant leaderboards (CSV)
# =========================
def _cached_csv(state: Dict[str, Any], key: str) -> Optional[str]:
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


def _save_cached_csv(state: Dict[str, Any], key: str, path: str) -> None:
    cf = state.get("cache_files", {})
    cf[key] = {"path": path, "fetched_utc": now_utc().isoformat()}
    state["cache_files"] = cf


def fetch_savant_leaderboard(year: int, which: str, state: Dict[str, Any]) -> pd.DataFrame:
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
    return pct if higher_is_better else (1 - pct)


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def k9(so: Any, ip: Any) -> Optional[float]:
    so_f = safe_float(so)
    ipf = innings_to_float(ip)
    if so_f is None or not ipf or ipf == 0:
        return None
    return 9 * so_f / ipf


def bb9(bb: Any, ip: Any) -> Optional[float]:
    bb_f = safe_float(bb)
    ipf = innings_to_float(ip)
    if bb_f is None or not ipf or ipf == 0:
        return None
    return 9 * bb_f / ipf


# =========================
# MiLB K% / BB% helpers
# =========================
def pct_str(num, den, digits: int = 1) -> str:
    try:
        n = float(num or 0)
        d = float(den or 0)
        if d <= 0:
            return ""
        return f"{(n / d) * 100:.{digits}f}%"
    except Exception:
        return ""


def milb_season_kbb_strings(stat: dict, is_pitcher: bool) -> tuple[str, str]:
    if not stat:
        return "", ""

    if is_pitcher:
        k = stat.get("strikeOuts")
        bb = stat.get("baseOnBalls")
        bf = stat.get("battersFaced")
        return pct_str(k, bf, digits=1), pct_str(bb, bf, digits=1)

    k = stat.get("strikeOuts")
    bb = stat.get("baseOnBalls")
    pa = stat.get("plateAppearances")
    return pct_str(k, pa, digits=1), pct_str(bb, pa, digits=1)


# =========================
# StatsAPI batch queries
# =========================
def _chunks(lst: List[int], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_statsapi_by_date_range(
    group: str, sport_id: int, person_ids: List[int], start_date: date, end_date: date
) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
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


def fetch_statsapi_season(group: str, sport_id: int, person_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
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
def hot_week_tables(
    roster_info: pd.DataFrame, pid_map: Dict[str, int], week_hit_stats: Dict[int, Dict[str, Any]], week_pit_stats: Dict[int, Dict[str, Any]]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hitters: List[Dict[str, Any]] = []
    sp_list: List[Dict[str, Any]] = []
    rp_list: List[Dict[str, Any]] = []

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
                hitters.append(
                    {
                        "Player": name,
                        "OPS": st.get("ops", ""),
                        "HR": st.get("homeRuns", ""),
                        "SB": st.get("stolenBases", ""),
                        "H": st.get("hits", ""),
                        "RBI": st.get("rbi", ""),
                    }
                )
        else:
            st = week_pit_stats.get(pid, {}) or {}
            gs = safe_float(st.get("gamesStarted"))
            era = safe_float(st.get("era"))
            sv = safe_float(st.get("saves"))
            hld = safe_float(st.get("holds"))

            if gs and gs >= 1:
                if era is not None and era < 1.50:
                    sp_list.append(
                        {
                            "Pitcher": name,
                            "GS": st.get("gamesStarted", ""),
                            "IP": st.get("inningsPitched", ""),
                            "ERA": st.get("era", ""),
                            "SO": st.get("strikeOuts", ""),
                            "BB": st.get("baseOnBalls", ""),
                        }
                    )
            else:
                total = (sv or 0) + (hld or 0)
                if total >= 3:
                    rp_list.append(
                        {
                            "Reliever": name,
                            "SV": st.get("saves", ""),
                            "HLD": st.get("holds", ""),
                            "IP": st.get("inningsPitched", ""),
                            "ERA": st.get("era", ""),
                            "SO": st.get("strikeOuts", ""),
                        }
                    )

    return pd.DataFrame(hitters), pd.DataFrame(sp_list), pd.DataFrame(rp_list)


# =========================
# Weekly date windows
# =========================
def previous_monday_sunday_window(now_local: datetime) -> Tuple[date, date]:
    this_monday = (now_local.date() - timedelta(days=now_local.weekday()))
    prev_monday = this_monday - timedelta(days=7)
    prev_sunday = this_monday - timedelta(days=1)
    return prev_monday, prev_sunday


def week_date_range_monday_sunday(now_local: datetime) -> Tuple[date, date]:
    monday = (now_local - timedelta(days=now_local.weekday())).date()
    sunday = monday + timedelta(days=6)
    return monday, sunday


# =========================
# Starting pitcher schedule / two-start pitchers
# =========================
def starting_pitcher_schedule_week(roster_df: pd.DataFrame) -> pd.DataFrame:
    roster_df = roster_df.copy()
    roster_df["position"] = roster_df.get("position", "").fillna("").astype(str)
    sp_names = roster_df.loc[roster_df["position"].str.contains("SP", case=False, na=False), "player_name"].tolist()
    sp_set = set(sp_names)
    if not sp_set:
        return pd.DataFrame(columns=["Date", "Day", "Pitcher", "Matchup", "Opponent", "Time (CT)", "Week Starts"])

    now_local = local_now()
    mon, sun = week_date_range_monday_sunday(now_local)

    rows: List[Dict[str, Any]] = []
    counts = {n: 0 for n in sp_set}

    d = mon
    while d <= sun:
        d_str = d.strftime("%Y-%m-%d")
        try:
            games = statsapi.schedule(date=d_str, sportId=1)
        except Exception:
            games = []

        for g in games:
            game_dt_utc = g.get("game_datetime")
            first_pitch_ct = ""
            if game_dt_utc:
                try:
                    dt = datetime.fromisoformat(game_dt_utc.replace("Z", "+00:00"))
                    ct = dt.astimezone(pytz.timezone(TZ_NAME))
                    first_pitch_ct = _fmt_local_time_safe(ct)
                except Exception:
                    first_pitch_ct = g.get("game_time", "") or ""

            away_pp = (g.get("away_probable_pitcher") or "").strip()
            home_pp = (g.get("home_probable_pitcher") or "").strip()
            away = g.get("away_name", "")
            home = g.get("home_name", "")

            if away_pp in sp_set:
                counts[away_pp] += 1
                rows.append(
                    {
                        "Date": d.strftime("%b %d"),
                        "Day": d.strftime("%a"),
                        "Pitcher": away_pp,
                        "Matchup": "at",
                        "Opponent": home,
                        "Time (CT)": first_pitch_ct,
                    }
                )
            if home_pp in sp_set:
                counts[home_pp] += 1
                rows.append(
                    {
                        "Date": d.strftime("%b %d"),
                        "Day": d.strftime("%a"),
                        "Pitcher": home_pp,
                        "Matchup": "vs",
                        "Opponent": away,
                        "Time (CT)": first_pitch_ct,
                    }
                )
        d += timedelta(days=1)

    for row in rows:
        row["Week Starts"] = counts.get(row["Pitcher"], 0)

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["Date", "Day", "Pitcher", "Matchup", "Opponent", "Time (CT)", "Week Starts"])

    df["sort_key"] = pd.to_datetime(df["Date"] + f" {local_now().year}", format="%b %d %Y", errors="coerce")
    df = df.sort_values(["sort_key", "Pitcher"]).drop(columns=["sort_key"]).reset_index(drop=True)
    return df


def two_start_pitchers_week(roster_df: pd.DataFrame) -> List[Dict[str, Any]]:
    sched = starting_pitcher_schedule_week(roster_df)
    if sched.empty:
        return []

    twos: List[Dict[str, Any]] = []
    for pitcher, sub in sched.groupby("Pitcher"):
        starts = int(sub["Week Starts"].iloc[0]) if "Week Starts" in sub.columns and not sub.empty else len(sub)
        if starts >= 2:
            details = [f"{r['Day']} {r['Matchup']} {r['Opponent']}" for _, r in sub.iterrows()]
            twos.append({"player": pitcher, "starts": starts, "details": details})
    twos.sort(key=lambda x: (-x["starts"], x["player"]))
    return twos


# =========================
# Draft-year filter logic
# =========================
def exclude_current_year_draft_pick(dd_signed_year: Any, has_pro_evidence: bool, year: int) -> bool:
    if dd_signed_year is None:
        return False
    try:
        if int(dd_signed_year) != int(year):
            return False
    except Exception:
        return False
    return not has_pro_evidence


# =========================
# Adds scoring: MLB + urgency
# =========================
def compute_waiver_urgency(add_score: Optional[float], opp_count: int, fills_need: bool, confidence: str) -> Tuple[int, str]:
    score = float(add_score or 0.0)
    points = 0.0

    if score >= 80:
        points += 3.0
    elif score >= 70:
        points += 2.2
    elif score >= 60:
        points += 1.4
    elif score >= 50:
        points += 0.8
    else:
        points += 0.3

    points += min(3.0, opp_count * 0.9)

    if fills_need:
        points += 1.0

    if confidence == "HIGH":
        points += 0.8
    elif confidence == "MEDIUM":
        points += 0.4

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

    why = "AddScore=%.1f" % score
    if opp_count:
        why += ", Opp=%s" % opp_count
    if fills_need:
        why += ", fills need"
    if confidence:
        why += ", %s" % confidence
    return urg, why


# =========================
# Major league adds
# =========================
def compute_major_league_adds(
    available_df: pd.DataFrame,
    top500_df: pd.DataFrame,
    savant_bat_df: pd.DataFrame,
    savant_pit_df: pd.DataFrame,
    recent_reports: List[Dict[str, Any]],
    state: Dict[str, Any],
    year: int,
    positions_of_need: List[str],
) -> pd.DataFrame:
    if available_df is None or available_df.empty:
        return pd.DataFrame()

    recent_reports = dedupe_reports_semantic(recent_reports)

    cand = available_df.copy()
    if top500_df is not None and not top500_df.empty:
        cand = cand.merge(top500_df, on="player_name", how="left")

    name_to_id: Dict[str, int] = {}
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

    def _join_savant(df: pd.DataFrame, sav_df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or sav_df is None or sav_df.empty:
            return df
        sav = sav_df.copy()
        cols_lower = {c.lower(): c for c in sav.columns}
        pid_col = cols_lower.get("player_id") or cols_lower.get("playerid")
        if pid_col:
            sav = sav.rename(columns={pid_col: "player_id"})
        sav["player_id_num"] = pd.to_numeric(sav.get("player_id"), errors="coerce")
        keep = ["player_id_num"]
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

    opp = compute_opportunity_signals(recent_reports, lookback_days=14)

    def opp_count(name: str) -> int:
        return int(opp.get(name, {}).get("count", 0))

    def opp_notes(name: str) -> str:
        d = opp.get(name, {})
        notes = d.get("notes", []) or []
        if not notes:
            return ""
        return " | ".join(notes[:2])

    def opp_best_conf(name: str) -> str:
        d = opp.get(name, {})
        conf = d.get("confidence", []) or []
        if "HIGH" in conf:
            return "HIGH"
        if "MEDIUM" in conf:
            return "MEDIUM"
        if conf:
            return conf[0]
        return ""

    def score_hitters(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["Add Score"] = []
            return df

        rank_num = pd.to_numeric(df.get("top500_rank"), errors="coerce")
        rank_pct = percentile_score(rank_num, higher_is_better=False).fillna(0)
        rank_score = 15 * rank_pct

        wrc = pd.to_numeric(df.get("wRC+"), errors="coerce")
        opsv = pd.to_numeric(df.get("OPS"), errors="coerce")
        hr = pd.to_numeric(df.get("HR"), errors="coerce")
        sb = pd.to_numeric(df.get("SB"), errors="coerce")

        perf_score = 60 * (
            0.45 * percentile_score(wrc, True).fillna(0) +
            0.25 * percentile_score(opsv, True).fillna(0) +
            0.20 * percentile_score(hr, True).fillna(0) +
            0.10 * percentile_score(sb, True).fillna(0)
        )

        sav_components = []
        for col in ["xwoba", "xslg", "hard_hit_percent", "barrel_batted_rate", "avg_exit_velocity"]:
            if col in df.columns:
                sav_components.append(percentile_score(df[col], True))
        sav_pct = pd.concat(sav_components, axis=1).mean(axis=1) if sav_components else pd.Series([0.0] * len(df), index=df.index)
        sav_score = 25 * sav_pct.fillna(0)

        df["Opportunity Count"] = df["Name"].apply(opp_count)
        df["Opportunity Notes"] = df["Name"].apply(opp_notes)
        df["Opportunity Confidence"] = df["Name"].apply(opp_best_conf)
        df["Opportunity Bonus"] = (df["Opportunity Count"].clip(upper=4) * 2).astype(float)
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

        sav_pct = pd.concat(sav_components, axis=1).mean(axis=1) if sav_components else pd.Series([0.0] * len(df), index=df.index)
        sav_score = 25 * sav_pct.fillna(0)

        df["Opportunity Count"] = df["Name"].apply(opp_count)
        df["Opportunity Notes"] = df["Name"].apply(opp_notes)
        df["Opportunity Confidence"] = df["Name"].apply(opp_best_conf)
        df["Opportunity Bonus"] = (df["Opportunity Count"].clip(upper=4) * 2).astype(float)
        df["Add Score"] = (rank_score + perf_score + sav_score + df["Opportunity Bonus"]).round(1)
        return df

    hitters = score_hitters(hitters)
    pitchers = score_pitchers(pitchers)

    scored = pd.concat([hitters, pitchers], ignore_index=True)
    scored["primary_pos"] = scored["Position"].apply(lambda x: _first_pos_for_sort(str(x)))
    scored["Savant"] = scored["pid_int"].apply(lambda x: button(baseball_savant_url(int(x)), "Savant", bg="#0b8043") if pd.notna(x) else "")

    used: Set[str] = set()
    forced_rows: List[pd.Series] = []
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

    final["Fills Need"] = final["primary_pos"].apply(lambda p: p in (positions_of_need or []))
    urg_list: List[int] = []
    why_list: List[str] = []
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
# Prospect adds (display simplified, scoring intact)
# =========================
def compute_prospect_adds(
    available_df: pd.DataFrame,
    dd_df: pd.DataFrame,
    bp_df: pd.DataFrame,
    recent_reports: List[Dict[str, Any]],
    state: Dict[str, Any],
    year: int
) -> pd.DataFrame:
    if available_df is None or available_df.empty:
        return pd.DataFrame()

    recent_reports = dedupe_reports_semantic(recent_reports)

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

    cand = cand[~cand.apply(lambda r: looks_like_asia(str(r.get("team_abbrev","")), str(r.get("Level",""))), axis=1)].copy()

    def is_draft_pick(row):
        sy = row.get("signed_year", None)
        has_pro = bool(row.get("pid")) or bool(str(row.get("Level","")).strip())
        return exclude_current_year_draft_pick(sy, has_pro, year)

    cand = cand[~cand.apply(is_draft_pick, axis=1)].copy()

    cand = cand[~cand["Level"].astype(str).str.upper().str.contains("MLB")].copy()
    if cand.empty:
        return pd.DataFrame()

    perf_raw = []
    k_pct_list = []
    bb_pct_list = []

    for _, r in cand.iterrows():
        pid = r.get("pid")
        if pd.isna(pid) or pid is None:
            perf_raw.append(0.0)
            k_pct_list.append("")
            bb_pct_list.append("")
            continue

        pid = int(pid)
        score = 0.0
        k_pct = ""
        bb_pct = ""

        try:
            hit = statsapi.get("stats", {"group": "hitting", "stats": "season", "sportId": 21, "personIds": str(pid)})
            splits = hit.get("stats", [])[0].get("splits", [])
            if splits:
                st = splits[0].get("stat", {}) or {}

                hr = float(st.get("homeRuns", 0) or 0)
                sb = float(st.get("stolenBases", 0) or 0)
                obp = st.get("obp")
                avg = st.get("avg")

                pa = st.get("plateAppearances")
                so = st.get("strikeOuts")
                bb = st.get("baseOnBalls")

                try:
                    pa_f = float(pa or 0)
                except Exception:
                    pa_f = 0.0
                try:
                    so_f = float(so or 0)
                except Exception:
                    so_f = 0.0
                try:
                    bb_f = float(bb or 0)
                except Exception:
                    bb_f = 0.0

                if pa_f > 0:
                    k_pct = f"{(so_f/pa_f)*100:.1f}%"
                    bb_pct = f"{(bb_f/pa_f)*100:.1f}%"

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
                    st = ps[0].get("stat", {}) or {}

                    ip = st.get("inningsPitched")
                    era = st.get("era")
                    so = float(st.get("strikeOuts", 0) or 0)
                    bb = float(st.get("baseOnBalls", 0) or 0)
                    bf = st.get("battersFaced")

                    try:
                        bf_f = float(bf or 0)
                    except Exception:
                        bf_f = 0.0
                    if bf_f > 0:
                        k_pct = f"{(so/bf_f)*100:.1f}%"
                        bb_pct = f"{(bb/bf_f)*100:.1f}%"

                    ipf = innings_to_float(ip) or 0.0
                    score = ipf*0.5 + so*0.25 - bb*0.1
                    try:
                        score += max(0.0, 8.0 - float(era)) * 2.5
                    except Exception:
                        pass

        except Exception:
            pass

        perf_raw.append(score)
        k_pct_list.append(k_pct)
        bb_pct_list.append(bb_pct)

    cand["perf_raw"] = perf_raw
    cand["K%"] = k_pct_list
    cand["BB%"] = bb_pct_list
    cand["perf_pct"] = percentile_score(cand["perf_raw"], True).fillna(0)

    cand["dd_rank_num"] = pd.to_numeric(cand.get("dd_rank"), errors="coerce")
    cand["bp_rank_num"] = pd.to_numeric(cand.get("bp_rank"), errors="coerce")
    dd_pct = percentile_score(cand["dd_rank_num"], higher_is_better=False).fillna(0)
    bp_pct = percentile_score(cand["bp_rank_num"], higher_is_better=False).fillna(0)

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
            if is_positive_opportunity_text(title):
                opp_hits[p] = opp_hits.get(p, 0) + 1

    cand["mentions_7d"] = cand["player_name"].map(mentions).fillna(0).astype(int)
    cand["opp_7d"] = cand["player_name"].map(opp_hits).fillna(0).astype(int)
    buzz_pct = ((cand["mentions_7d"].clip(upper=5) + cand["opp_7d"].clip(upper=3)) / 8.0).fillna(0)

    cand["Add Score"] = (30*dd_pct + 30*bp_pct + 30*cand["perf_pct"] + 10*buzz_pct).round(1)

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
        if score >= 80:
            pts += 2.7
        elif score >= 70:
            pts += 2.0
        elif score >= 60:
            pts += 1.3
        elif score >= 50:
            pts += 0.7
        else:
            pts += 0.3
        pts += level_bonus(r.get("Level",""))
        pts += min(1.8, int(r.get("opp_7d", 0) or 0) * 0.6)
        pts += min(1.0, int(r.get("mentions_7d", 0) or 0) * 0.15)

        if pts >= 5.4:
            urg = 5
        elif pts >= 4.3:
            urg = 4
        elif pts >= 3.2:
            urg = 3
        elif pts >= 2.2:
            urg = 2
        else:
            urg = 1
        urg_vals.append(urg)

    cand["Urgency"] = urg_vals

    out = cand.rename(columns={"player_name": "Name", "team_abbrev": "Team", "position": "Position", "age": "Age"})
    out = out.sort_values("Add Score", ascending=False).head(10).copy()

    out["Savant"] = out["pid_int"].apply(
        lambda x: button(baseball_savant_url(int(x)), "Savant", bg="#0b8043") if pd.notna(x) else ""
    )
    out["B-Ref"] = out["Name"].apply(
        lambda x: button(baseball_reference_search_url(str(x)), "B-Ref", bg="#5f6368") if str(x).strip() else ""
    )

    out = out[[
        "Name", "Team", "Level", "Age", "Position",
        "dd_rank", "bp_rank",
        "Add Score", "Urgency", "Savant", "B-Ref"
    ]]

    out = out.rename(columns={
        "dd_rank": "Dynasty Dugout",
        "bp_rank": "Baseball Prospectus",
    })

    def _int_no_decimal(x):
        s = str(x or "").strip()
        if not s:
            return ""
        try:
            return str(int(float(s)))
        except Exception:
            return s

    if "Dynasty Dugout" in out.columns:
        out["Dynasty Dugout"] = out["Dynasty Dugout"].apply(_int_no_decimal)
    if "Baseball Prospectus" in out.columns:
        out["Baseball Prospectus"] = out["Baseball Prospectus"].apply(_int_no_decimal)

    return out


# =========================
# Daily email builder
# =========================
def build_daily_bodies(
    official_items: List[Dict[str, Any]],
    starters: List[Dict[str, str]],
    reports: List[Dict[str, Any]],
    opp_alerts: List[Dict[str, Any]],
    mlb_adds_df: pd.DataFrame,
    roster_df: pd.DataFrame,
    title_str: str,
) -> Tuple[str, str]:
    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    text: List[str] = []
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
            text.append(f"- [{a['confidence']}] {hdr}: {a['net']} ({a['source']}) {a['link']}".strip())

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

    html: List[str] = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 8px 0;'>Dynasty Daily Update — {h(title_str)}</h2>")

    html.append(section_header("Transaction Wire (Official)", "#0b8043"))
    if official_items:
        by_player: Dict[str, List[Dict[str, Any]]] = {}
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
                f"<div style='margin-top:6px;'><span style='font-weight:700;'>[{h(a['confidence'])}]</span> {h(a['net'])}</div>"
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
        by_player2: Dict[str, List[Dict[str, Any]]] = {}
        for r in reports:
            by_player2.setdefault(r["player"], []).append(r)
        for player in sorted(by_player2.keys()):
            tm = team_by_player.get(player, "")
            hdr = f"{player} ({tm})" if tm else player
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                f"<div style='font-size:16px; margin-bottom:8px;'><b>{h(hdr)}</b></div>"
            )
            for r in by_player2[player]:
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
# Spring Training module (roster batting lines only)
# =========================
def is_spring_training_season(now_local: Optional[datetime] = None) -> bool:
    now_local = now_local or local_now()
    if now_local.month not in SPRING_TRAINING_MONTHS:
        return False

    try:
        yday = (now_local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
        games = statsapi.schedule(date=yday, sportId=1, gameType=SPRING_GAME_TYPE)
        return bool(games)
    except Exception:
        return True


def fetch_yesterdays_spring_training_batting_lines(roster_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if roster_df is None or roster_df.empty:
        return []

    roster_names = set(roster_df["player_name"].astype(str).tolist())
    team_by_player = dict(zip(roster_df["player_name"].astype(str), roster_df.get("team_abbrev", "").astype(str)))

    yday = (local_now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        games = statsapi.schedule(date=yday, sportId=1, gameType=SPRING_GAME_TYPE)
    except Exception:
        games = []

    rows: List[Dict[str, Any]] = []

    for g in games:
        game_pk = g.get("game_id") or g.get("game_pk") or g.get("gamePk")
        if not game_pk:
            continue
        try:
            game_pk = int(game_pk)
        except Exception:
            continue

        try:
            bs = statsapi.boxscore_data(game_pk)
        except Exception:
            continue

        away_team = (
            bs.get("teamInfo", {}).get("away", {}).get("abbreviation")
            or bs.get("teamInfo", {}).get("away", {}).get("name")
            or g.get("away_name", "")
        )
        home_team = (
            bs.get("teamInfo", {}).get("home", {}).get("abbreviation")
            or bs.get("teamInfo", {}).get("home", {}).get("name")
            or g.get("home_name", "")
        )

        sides = [
            ("awayBatters", away_team, home_team),
            ("homeBatters", home_team, away_team),
        ]

        for side_key, team_name, opp_name in sides:
            for row in (bs.get(side_key, []) or []):
                nm = str(row.get("name", "")).strip()
                if not nm or nm not in roster_names:
                    continue

                ab = str(row.get("ab", "")).strip()
                h_ = str(row.get("h", "")).strip()
                hr = str(row.get("hr", "")).strip()
                rbi = str(row.get("rbi", "")).strip()
                bb = str(row.get("bb", "")).strip()
                so = str(row.get("so", "")).strip()
                sb = str(row.get("sb", "")).strip()

                stat_blob = f"{ab}{h_}{hr}{rbi}{bb}{so}{sb}".strip()
                if not stat_blob:
                    continue

                rows.append(
                    {
                        "Player": nm,
                        "Team": team_by_player.get(nm, "") or team_name,
                        "Opponent": opp_name,
                        "AB": ab,
                        "H": h_,
                        "HR": hr,
                        "RBI": rbi,
                        "BB": bb,
                        "K": so,
                        "SB": sb,
                    }
                )

    rows = sorted(rows, key=lambda x: (x.get("Player", ""), x.get("Team", ""), x.get("Opponent", "")))
    return rows


def build_spring_training_batting_email(rows: List[Dict[str, Any]]) -> Tuple[str, str]:
    date_str = (local_now().date() - timedelta(days=1)).strftime("%Y-%m-%d")

    if not rows:
        text = f"Spring Training Batting Lines — {date_str}\n\nNo roster batting lines found yesterday."
        html = (
            "<html><body style='font-family:Arial, Helvetica, sans-serif; color:#111;'>"
            f"<h2 style='margin:0 0 8px 0;'>Spring Training Batting Lines — {h(date_str)}</h2>"
            "<div style='color:#666;'>No roster batting lines found yesterday.</div>"
            "</body></html>"
        )
        return text, html

    text_lines = [f"Spring Training Batting Lines — {date_str}", ""]
    for r in rows:
        text_lines.append(
            f"- {r['Player']} ({r['Team']}) vs {r['Opponent']}: AB {r['AB']}, H {r['H']}, HR {r['HR']}, RBI {r['RBI']}, BB {r['BB']}, K {r['K']}, SB {r['SB']}"
        )
    text_body = "\n".join(text_lines)

    df = pd.DataFrame(rows)[["Player", "Team", "Opponent", "AB", "H", "HR", "RBI", "BB", "K", "SB"]]

    html: List[str] = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 8px 0;'>Spring Training Batting Lines — {h(date_str)}</h2>")
    html.append(render_table_html(df, "Roster Batting Lines", html_cols=set()))
    html.append("</body></html>")
    return text_body, "".join(html)


def run_spring_training_daily_allgames() -> None:
    if not is_spring_training_season(local_now()):
        print("[spring] Not in spring training window; skipping.")
        return

    log("[daily] loading roster...")
    roster_df = load_roster()
    log(f"[daily] roster loaded rows={len(roster_df)}")
    rows = fetch_yesterdays_spring_training_batting_lines(roster_df)
    subj_date = (local_now().date() - timedelta(days=1)).strftime("%b %d")
    subject = f"Spring Training Batting Lines — {subj_date}"

    text_body, html_body = build_spring_training_batting_email(rows)
    send_email(subject, text_body, html_body)


# =========================
# Daily runner
# =========================
def run_daily(lookback_hours: Optional[int] = None) -> None:
    log("[daily] ENTER run_daily")
    state = load_state()
    removed = scrub_bad_player_cache(state)
    if removed:
        log(f"[cache] scrubbed {removed} bad entries from player_cache")
        save_state(state)

    roster_df = load_roster()
    roster = roster_df["player_name"].tolist()
    year = local_now().year

    log(f"[roster] players={len(roster)} sample={roster[:10]}")

    if env_flag("IS_SCHEDULED") and not env_flag("FORCE_RUN"):
        ok, reason = is_daily_time(state)
        if not ok:
            log(f"[daily] Skipping - {reason}")
            save_state(state)
            return

    since = now_utc() - timedelta(hours=lookback_hours or 24)
    log(f"[daily] since={since.isoformat()}")

    official_items: List[Dict[str, Any]] = []
    for i, player in enumerate(roster):
        if i % 10 == 0:
            log(f"[daily] tx progress {i}/{len(roster)}")
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        tx = tx_since(fetch_transactions(pid), since)
        for t in tx:
            official_items.append({"player": player, "desc": t["desc"], "utc": t["utc"]})
            append_jsonl(WEEKLY_OFFICIAL_PATH, {"player": player, **t})

    log("[daily] starting RSS/news fetch (last 7 days only, de-duped)...")
    reports = dedupe_reports_semantic(fetch_reports(roster, state, max_age_days=7))
    for r in reports:
        append_jsonl(WEEKLY_REPORTS_PATH, r)

    starters = todays_starters_for_roster(roster_df)

    opp_alerts: List[Dict[str, Any]] = []
    for r in reports:
        if is_positive_opportunity_text(r.get("title", "")):
            opp_alerts.append(
                {
                    "player": r["player"],
                    "confidence": opportunity_confidence(r.get("title", "")),
                    "net": summarize_opportunity_net(r.get("title", "")),
                    "source": r.get("source", ""),
                    "link": r.get("link", ""),
                }
            )
    opp_alerts = opp_alerts[:12]

    mlb_adds_df = pd.DataFrame()
    include_adds = (os.getenv("IS_SCHEDULED", "0") == "1" and should_include_midweek_adds_now()) or (os.getenv("RUN_MODE", "") == "daily_realnews_test")
    if include_adds:
        try:
            available_df = load_available_players()
            top500_df = load_top500_dynasty_rankings()
            sav_bat = fetch_savant_leaderboard(year, "batter", state)
            sav_pit = fetch_savant_leaderboard(year, "pitcher", state)
            positions_of_need = parse_positions_of_need_from_roster(roster_df)
            mlb_adds_df = compute_major_league_adds(available_df, top500_df, sav_bat, sav_pit, reports, state, year, positions_of_need)
        except Exception as e:
            log(f"[daily] MLB Adds error: {e}")
            mlb_adds_df = pd.DataFrame()

    log(f"[daily] official_items={len(official_items)} reports_items={len(reports)} starters={len(starters)} opp_alerts={len(opp_alerts)} adds_rows={len(mlb_adds_df) if mlb_adds_df is not None else 0}")

    any_adds = mlb_adds_df is not None and not mlb_adds_df.empty
    if not official_items and not reports and not starters and not any_adds and not opp_alerts:
        log("[daily] no content found; sending empty digest")
        title_str = local_now().strftime("%b %d")
        text_body = f"Dynasty Daily Update — {title_str}\n\nNo qualifying items found this morning."
        html_body = (
            "<html><body style='font-family:Arial, Helvetica, sans-serif; color:#111;'>"
            f"<h2>Dynasty Daily Update — {h(title_str)}</h2>"
            "<div style='color:#666;'>No qualifying items found this morning.</div>"
            "</body></html>"
        )
        send_email(f"Dynasty Daily Update — {title_str}", text_body, html_body)

        if env_flag("IS_SCHEDULED"):
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

    if env_flag("IS_SCHEDULED"):
        mark_daily_sent(state)

    save_state(state)


# =========================
# Weekly gates
# =========================
def should_send_weekly_now() -> bool:
    ln = local_now()
    return ln.weekday() == 0 and ln.hour == 7  # Monday 7am CT


def mark_weekly_sent(state: Dict[str, Any]) -> None:
    state["last_weekly_local_date"] = local_now().strftime("%Y-%m-%d")


# =========================
# Weekly row helpers
# =========================
def hitter_row(name, team, level, pos, pid, wk, ss, injury_players, fg_adv=None, dd_rank=""):
    status = build_status_html(name, injury_players, wk or {}, is_pitcher=False)

    if fg_adv:
        season_k = (fg_adv or {}).get("K%", "") or ""
        season_bb = (fg_adv or {}).get("BB%", "") or ""
    else:
        season_k, season_bb = milb_season_kbb_strings(ss or {}, is_pitcher=False)

    return {
        "Player": name,
        "Team": team,
        "Level": level,
        "Position": pos,
        "DD Rank": dd_rank,

        "W G": (wk or {}).get("gamesPlayed", ""),
        "W H": (wk or {}).get("hits", ""),
        "W HR": (wk or {}).get("homeRuns", ""),
        "W RBI": (wk or {}).get("rbi", ""),
        "W SB": (wk or {}).get("stolenBases", ""),
        "W AVG": (wk or {}).get("avg", ""),
        "W OBP": (wk or {}).get("obp", ""),
        "W OPS": (wk or {}).get("ops", ""),

        "Status": status,

        "S G": (ss or {}).get("gamesPlayed", ""),
        "S H": (ss or {}).get("hits", ""),
        "S HR": (ss or {}).get("homeRuns", ""),
        "S RBI": (ss or {}).get("rbi", ""),
        "S SB": (ss or {}).get("stolenBases", ""),
        "S AVG": (ss or {}).get("avg", ""),
        "S OBP": (ss or {}).get("obp", ""),
        "S OPS": (ss or {}).get("ops", ""),

        "S wRC+": (fg_adv or {}).get("wRC+", "") if fg_adv else "",
        "S K%": season_k,
        "S BB%": season_bb,

        "Savant": button(baseball_savant_url(int(pid)), "Savant", bg="#0b8043") if pd.notna(pid) else "",
    }


def pitcher_row(name, team, level, pos, pid, wk, ss, injury_players, fg_adv=None, dd_rank=""):
    status = build_status_html(name, injury_players, wk or {}, is_pitcher=True)

    ip_season = (ss or {}).get("inningsPitched", "")
    so_season = (ss or {}).get("strikeOuts", "")
    bb_season = (ss or {}).get("baseOnBalls", "")
    k9v = k9(so_season, ip_season)
    bb9v = bb9(bb_season, ip_season)

    if fg_adv:
        season_fip = (fg_adv or {}).get("FIP", "") or ""
        season_k = (fg_adv or {}).get("K%", "") or ""
        season_bb = (fg_adv or {}).get("BB%", "") or ""
    else:
        season_fip = ""
        season_k, season_bb = milb_season_kbb_strings(ss or {}, is_pitcher=True)

    return {
        "Pitcher": name,
        "Team": team,
        "Level": level,
        "Position": pos,
        "DD Rank": dd_rank,

        "W GS": (wk or {}).get("gamesStarted", ""),
        "W IP": (wk or {}).get("inningsPitched", ""),
        "W ERA": (wk or {}).get("era", ""),
        "W SO": (wk or {}).get("strikeOuts", ""),
        "W BB": (wk or {}).get("baseOnBalls", ""),

        "Status": status,

        "S Starts": (ss or {}).get("gamesStarted", ""),
        "S Innings": (ss or {}).get("inningsPitched", ""),
        "S FIP": season_fip,
        "S K%": season_k,
        "S BB%": season_bb,
        "S K/9": f"{k9v:.2f}" if k9v is not None else "",
        "S BB/9": f"{bb9v:.2f}" if bb9v is not None else "",

        "Savant": button(baseball_savant_url(int(pid)), "Savant", bg="#0b8043") if pd.notna(pid) else "",
    }


# =========================
# Weekly runner
# =========================
def run_weekly(force: bool = False) -> None:
    log("[weekly] ENTER run_weekly")
    log(f"[weekly] force={force} RUN_MODE={os.getenv('RUN_MODE', '')} IS_SCHEDULED={os.getenv('IS_SCHEDULED', '')}")

    state = load_state()
    removed = scrub_bad_player_cache(state)
    if removed:
        log(f"[cache] scrubbed {removed} bad entries from player_cache")
        save_state(state)

    now_local = local_now()
    year = now_local.year

    if os.getenv("IS_SCHEDULED", "0") == "1" and not force:
        if not should_send_weekly_now():
            log("[weekly] Skipping - not Monday 7am CT.")
            save_state(state)
            return
        today = now_local.strftime("%Y-%m-%d")
        if state.get("last_weekly_local_date") == today:
            log("[weekly] Already sent today.")
            save_state(state)
            return

    roster_df = load_roster()
    roster_df["position"] = roster_df.get("position", "").fillna("").astype(str)
    roster_names = roster_df["player_name"].tolist()

    name_to_id: Dict[str, int] = {}
    for nm in roster_names:
        pid = lookup_mlbam_id(nm, state)
        if pid:
            name_to_id[nm] = pid
    save_state(state)

    roster_pids = [int(x) for x in name_to_id.values() if x]

    official_news = read_jsonl(WEEKLY_OFFICIAL_PATH)
    reports_news = dedupe_reports_semantic(read_jsonl(WEEKLY_REPORTS_PATH))

    w_start, w_end = previous_monday_sunday_window(now_local)

    week_hit_mlb = fetch_statsapi_by_date_range("hitting", SPORT_ID_MLB, roster_pids, w_start, w_end)
    week_pit_mlb = fetch_statsapi_by_date_range("pitching", SPORT_ID_MLB, roster_pids, w_start, w_end)
    week_hit_milb = fetch_statsapi_by_date_range("hitting", SPORT_ID_MILB, roster_pids, w_start, w_end)
    week_pit_milb = fetch_statsapi_by_date_range("pitching", SPORT_ID_MILB, roster_pids, w_start, w_end)

    season_hit_mlb = fetch_statsapi_season("hitting", SPORT_ID_MLB, roster_pids)
    season_pit_mlb = fetch_statsapi_season("pitching", SPORT_ID_MLB, roster_pids)
    season_hit_milb = fetch_statsapi_season("hitting", SPORT_ID_MILB, roster_pids)
    season_pit_milb = fetch_statsapi_season("pitching", SPORT_ID_MILB, roster_pids)

    try:
        fg_hit = batting_stats(year)[["Name", "Team", "G", "H", "HR", "RBI", "SB", "AVG", "OBP", "OPS", "wRC+", "K%", "BB%"]].copy()
    except Exception:
        fg_hit = pd.DataFrame(columns=["Name"])

    try:
        fg_pit = pitching_stats(year)[["Name", "Team", "GS", "IP", "ERA", "FIP", "K%", "BB%", "SV", "HLD"]].copy()
    except Exception:
        fg_pit = pd.DataFrame(columns=["Name"])

    fg_hit_map: Dict[str, Dict[str, Any]] = {}
    if not fg_hit.empty:
        for _, r in fg_hit.iterrows():
            fg_hit_map[str(r.get("Name", "")).strip()] = {k: r.get(k, "") for k in ["wRC+", "K%", "BB%", "OPS"]}

    fg_pit_map: Dict[str, Dict[str, Any]] = {}
    if not fg_pit.empty:
        for _, r in fg_pit.iterrows():
            fg_pit_map[str(r.get("Name", "")).strip()] = {k: r.get(k, "") for k in ["FIP", "K%", "BB%"]}

    roster_info = roster_df.copy()
    roster_info["pid"] = roster_info["player_name"].map(name_to_id)
    roster_info["Level"] = roster_info["pid"].apply(lambda x: infer_player_level(int(x), year) if pd.notna(x) and x else "")
    roster_info["is_pitcher"] = roster_info["position"].apply(is_pitcher_position)
    roster_info["is_mlb"] = roster_info["Level"].astype(str).str.upper().str.contains("MLB")

    dd_df = load_dynasty_dugout_rankings()
    dd_rank_map = {}
    if dd_df is not None and not dd_df.empty:
        dd_rank_map = dict(zip(dd_df["player_name"].tolist(), dd_df["dd_rank"].tolist()))

    injury_players: Set[str] = set()
    injury_cards: List[Dict[str, Any]] = []
    for it in official_news:
        if it.get("player") and is_injury_text(it.get("desc", "")):
            injury_players.add(it["player"])
            injury_cards.append({"player": it["player"], "text": it.get("desc", ""), "source": "Official", "link": ""})
    for it in reports_news:
        if it.get("player") and is_injury_text(it.get("title", "")):
            injury_players.add(it["player"])
            injury_cards.append({"player": it["player"], "text": it.get("title", ""), "source": it.get("source", "Report"), "link": it.get("link", "")})

    opp_map = compute_opportunity_signals(reports_news, lookback_days=14)
    opp_alerts_weekly: List[Dict[str, Any]] = []
    for p, d in opp_map.items():
        if p in roster_names and d.get("notes"):
            conf_list = d.get("confidence") or []
            conf = "HIGH" if "HIGH" in conf_list else ("MEDIUM" if "MEDIUM" in conf_list else "LOW")
            opp_alerts_weekly.append({"player": p, "confidence": conf, "notes": d.get("notes", [])[:2], "links": d.get("links", [])[:2]})
    opp_alerts_weekly.sort(key=lambda x: ({"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(x["confidence"], 9), x["player"]))

    sp_schedule_df = starting_pitcher_schedule_week(roster_df)
    two_start_list = two_start_pitchers_week(roster_df)

    hot_hit_df, hot_sp_df, hot_rp_df = hot_week_tables(
        roster_info[["player_name", "position"]],
        name_to_id,
        {**week_hit_mlb, **week_hit_milb},
        {**week_pit_mlb, **week_pit_milb},
    )

    hitters_rows: List[Dict[str, Any]] = []
    for _, r in roster_info.iterrows():
        if r["is_pitcher"]:
            continue

        nm = r["player_name"]
        tm = r.get("team_abbrev", "")
        pos = r.get("position", "")
        pid = r.get("pid")
        level = "MLB" if r["is_mlb"] else r.get("Level", "")
        dd_rank = dd_rank_map.get(nm, "")

        if r["is_mlb"]:
            wk = week_hit_mlb.get(pid, {}) if pid else {}
            ss = season_hit_mlb.get(pid, {}) if pid else {}
            hitters_rows.append(
                hitter_row(nm, tm, "MLB", pos, pid, wk, ss, injury_players, fg_hit_map.get(nm, {}), dd_rank="")
            )
        else:
            wk = week_hit_milb.get(pid, {}) if pid else {}
            ss = season_hit_milb.get(pid, {}) if pid else {}
            hitters_rows.append(
                hitter_row(nm, tm, level, pos, pid, wk, ss, injury_players, None, dd_rank=dd_rank)
            )

    hitters_df = pd.DataFrame(hitters_rows)

    if hitters_df.empty:
        hitters_mlb = pd.DataFrame()
        hitters_milb = pd.DataFrame()
    else:
        hitter_cols_base = ["Player", "Team", "Level", "Position"]

        hitter_week_cols = ["W G", "W H", "W HR", "W RBI", "W SB", "W AVG", "W OBP", "W OPS"]
        hitter_status_col = ["Status"]
        hitter_season_cols = ["S G", "S H", "S HR", "S RBI", "S SB", "S AVG", "S OBP", "S OPS", "S wRC+", "S K%", "S BB%", "Savant"]

        hitter_cols_mlb = hitter_cols_base + hitter_week_cols + hitter_status_col + hitter_season_cols
        hitter_cols_milb = hitter_cols_base + ["DD Rank"] + hitter_week_cols + hitter_status_col + hitter_season_cols

        hitters_df["is_mlb"] = hitters_df["Level"].astype(str).str.upper().str.contains("MLB")
        hitters_df["pos_key"] = hitters_df["Position"].apply(_pos_sort_key_mlb)

        hitters_mlb = (
            hitters_df[hitters_df["is_mlb"]][[c for c in hitter_cols_mlb if c in hitters_df.columns] + ["pos_key"]]
            .sort_values(["pos_key", "Player"])
            .drop(columns=["pos_key"])
            .reset_index(drop=True)
        )

        hitters_milb = (
            hitters_df[~hitters_df["is_mlb"]][[c for c in hitter_cols_milb if c in hitters_df.columns]]
            .sort_values(["Player"])
            .reset_index(drop=True)
        )

    pitchers_rows: List[Dict[str, Any]] = []
    for _, r in roster_info.iterrows():
        if not r["is_pitcher"]:
            continue
        nm = r["player_name"]
        tm = r.get("team_abbrev", "")
        pos = r.get("position", "")
        pid = r.get("pid")
        level = "MLB" if r["is_mlb"] else r.get("Level", "")
        dd_rank = dd_rank_map.get(nm, "")

        if r["is_mlb"]:
            wk = week_pit_mlb.get(pid, {}) if pid else {}
            ss = season_pit_mlb.get(pid, {}) if pid else {}
            pitchers_rows.append(pitcher_row(nm, tm, "MLB", pos, pid, wk, ss, injury_players, fg_pit_map.get(nm, {}), dd_rank=""))
        else:
            wk = week_pit_milb.get(pid, {}) if pid else {}
            ss = season_pit_milb.get(pid, {}) if pid else {}
            pitchers_rows.append(pitcher_row(nm, tm, level, pos, pid, wk, ss, injury_players, None, dd_rank=dd_rank))

    pitchers_df = pd.DataFrame(pitchers_rows)
    if not pitchers_df.empty:
        pitchers_df["is_mlb"] = pitchers_df["Level"].astype(str).str.upper().str.contains("MLB")

        pitcher_cols_base = ["Pitcher", "Team", "Level", "Position"]
        pitcher_week_cols = ["W GS", "W IP", "W ERA", "W SO", "W BB"]
        pitcher_status_col = ["Status"]
        pitcher_season_cols = ["S Starts", "S Innings", "S FIP", "S K%", "S BB%", "S K/9", "S BB/9", "Savant"]

        pitcher_cols_mlb = pitcher_cols_base + pitcher_week_cols + pitcher_status_col + pitcher_season_cols
        pitcher_cols_milb = pitcher_cols_base + ["DD Rank"] + pitcher_week_cols + pitcher_status_col + pitcher_season_cols

        pit_mlb = (
            pitchers_df[pitchers_df["is_mlb"]][[c for c in pitcher_cols_mlb if c in pitchers_df.columns]]
            .sort_values(["Pitcher"])
            .reset_index(drop=True)
        )

        pit_milb = (
            pitchers_df[~pitchers_df["is_mlb"]][[c for c in pitcher_cols_milb if c in pitchers_df.columns]]
            .sort_values(["Pitcher"])
            .reset_index(drop=True)
        )
    else:
        pit_mlb = pd.DataFrame()
        pit_milb = pd.DataFrame()

    positions_of_need = parse_positions_of_need_from_roster(roster_df)
    available_df = load_available_players()
    bp_df = load_baseball_prospectus_rankings()
    top500_df = load_top500_dynasty_rankings()
    sav_bat = fetch_savant_leaderboard(year, "batter", state)
    sav_pit = fetch_savant_leaderboard(year, "pitcher", state)

    mlb_adds_df = compute_major_league_adds(available_df, top500_df, sav_bat, sav_pit, reports_news, state, year, positions_of_need)
    prospect_adds_df = compute_prospect_adds(available_df, dd_df, bp_df, reports_news, state, year)

    filtered_reports = [r for r in reports_news if not (r.get("player") in injury_players and is_injury_text(r.get("title", "")))]

    subject = f"Dynasty Weekly Report — {now_local.strftime('%b %d, %Y')}"
    text_body = f"Dynasty Weekly Report ({w_start} to {w_end})\n(See HTML version for full formatting.)"

    team_by_player = dict(zip(roster_df["player_name"].tolist(), roster_df["team_abbrev"].tolist()))

    html: List[str] = []
    html.append("<html><body style='font-family:Arial, Helvetica, sans-serif; line-height:1.35; color:#111;'>")
    html.append(f"<h2 style='margin:0 0 8px 0;'>Dynasty Weekly Report — {h(now_local.strftime('%b %d, %Y'))}</h2>")
    html.append(f"<div style='color:#666; margin-bottom:6px;'>Stat window: <b>{h(w_start.strftime('%b %d'))} – {h(w_end.strftime('%b %d'))}</b></div>")

    html.append(section_header("Transaction Wire (Official)", "#0b8043"))
    if not official_news:
        html.append("<div style='color:#666;'>No official transactions logged this week.</div>")
    else:
        byp: Dict[str, List[Dict[str, Any]]] = {}
        for it in official_news:
            byp.setdefault(it.get("player", ""), []).append(it)
        for p in sorted([x for x in byp.keys() if x]):
            tm = team_by_player.get(p, "")
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt='{h(p)}'/>" if pid else ""
            tid = team_id_from_abbrev(tm, state)
            logo = f"<img src='{h(mlb_team_logo_url(tid))}' width='22' height='22' style='vertical-align:middle;' alt='{h(tm)}'/>" if tid else ""
            hdr = f"{p} ({tm})" if tm else p

            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div>"
                f"<div style='margin-left:auto;'>{logo}</div>"
                "</div><ul style='margin:10px 0 0 0; padding-left:18px;'>"
            )
            for it in byp[p]:
                html.append(f"<li style='margin:6px 0;'>{h(it.get('desc', ''))}</li>")
            html.append("</ul></div>")

    html.append(section_header("Injury Watch", "#d93025"))
    if not injury_cards:
        html.append("<div style='color:#666;'>No injuries detected in logged items this week.</div>")
    else:
        byp2: Dict[str, List[Dict[str, Any]]] = {}
        for it in injury_cards:
            byp2.setdefault(it["player"], []).append(it)
        for p in sorted(byp2.keys()):
            tm = team_by_player.get(p, "")
            hdr = f"{p} ({tm})" if tm else p
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt='{h(p)}'/>" if pid else ""
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #ffe3e0; background:#fff5f4; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div></div>"
            )
            for it in byp2[p]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #ffd3ce;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(it.get('text',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#6b2b22; font-size:13px;'>Source: {h(it.get('source',''))}</span>"
                    f"{button(it.get('link',''), 'News', bg='#d93025') if it.get('link') else ''}"
                    "</div></div>"
                )
            html.append("</div>")

    html.append(section_header("Starting Pitcher Schedule", "#1a73e8"))
    if sp_schedule_df is None or sp_schedule_df.empty:
        html.append("<div style='color:#666;'>No scheduled starts found for rostered starting pitchers yet.</div>")
    else:
        html.append(render_table_html(sp_schedule_df, "Projected Starts This Week", html_cols=set()))

    html.append(section_header("Two-Start Pitchers", "#1a73e8"))
    if not two_start_list:
        html.append("<div style='color:#666;'>No 2-start probables detected (or probables not posted yet).</div>")
    else:
        html.append("<ul style='margin:0; padding-left:18px;'>")
        for t in two_start_list:
            det = "; ".join(t.get("details", [])[:6])
            html.append(f"<li style='margin:6px 0;'><b>{h(t['player'])}</b> — {h(str(t['starts']))} starts. <span style='color:#555'>{h(det)}</span></li>")
        html.append("</ul>")

    if (hot_hit_df is not None and not hot_hit_df.empty) or (hot_sp_df is not None and not hot_sp_df.empty) or (hot_rp_df is not None and not hot_rp_df.empty):
        html.append(section_header("Hot Week Performances", "#f9ab00"))
        if hot_hit_df is not None and not hot_hit_df.empty:
            html.append(render_table_html(hot_hit_df, "Hot Hitters", html_cols=set()))
        if hot_sp_df is not None and not hot_sp_df.empty:
            html.append(render_table_html(hot_sp_df, "Hot Starters", html_cols=set()))
        if hot_rp_df is not None and not hot_rp_df.empty:
            html.append(render_table_html(hot_rp_df, "Hot Relievers", html_cols=set()))

    html.append("<div style='border-top:2px solid #d0d0d0; margin:16px 0;'></div>")
    if not hitters_mlb.empty:
        html.append(render_table_html(hitters_mlb, "MLB Hitters (sorted by position)", html_cols={"Status", "Savant"}))
    else:
        html.append("<div style='color:#666;'>No MLB hitters found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")
    if not hitters_milb.empty:
        html.append(render_table_html(hitters_milb, "Minor League Hitters (alphabetical)", html_cols={"Status", "Savant"}))
    else:
        html.append("<div style='color:#666;'>No minor-league hitters found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")
    if not pit_mlb.empty:
        html.append(render_table_html(pit_mlb, "MLB Pitchers", html_cols={"Status", "Savant"}))
    else:
        html.append("<div style='color:#666;'>No MLB pitchers found.</div>")

    html.append("<div style='border-top:2px dashed #e0e0e0; margin:12px 0;'></div>")
    if not pit_milb.empty:
        html.append(render_table_html(pit_milb, "Minor League Pitchers", html_cols={"Status", "Savant"}))
    else:
        html.append("<div style='color:#666;'>No minor-league pitchers found.</div>")

    html.append(section_header("Major News From The Week", "#5f6368"))
    if not filtered_reports:
        html.append("<div style='color:#666;'>No matched reports logged this week.</div>")
    else:
        byp3: Dict[str, List[Dict[str, Any]]] = {}
        for r in sorted(filtered_reports, key=lambda x: (x.get("player", ""), x.get("source", ""), x.get("title", ""))):
            byp3.setdefault(r.get("player", ""), []).append(r)
        for p in sorted([x for x in byp3.keys() if x]):
            tm = team_by_player.get(p, "")
            hdr = f"{p} ({tm})" if tm else p
            pid = name_to_id.get(p)
            head = f"<img src='{h(mlb_headshot_url(pid))}' width='40' height='40' style='border-radius:999px;' alt='{h(p)}'/>" if pid else ""
            html.append(
                "<div style='margin:12px 0; padding:12px 14px; border:1px solid #e8e8e8; border-radius:12px;'>"
                "<div style='display:flex; gap:10px; align-items:center;'>"
                f"{head}<div style='font-size:16px;'><b>{h(hdr)}</b></div></div>"
            )
            for rr in byp3[p]:
                html.append(
                    "<div style='margin:10px 0 0 0; padding-top:10px; border-top:1px solid #f0f0f0;'>"
                    f"<div style='margin:0 0 6px 0;'>{h(rr.get('title',''))}</div>"
                    "<div style='display:flex; gap:10px; align-items:center; flex-wrap:wrap;'>"
                    f"<span style='color:#555; font-size:13px;'>Source: {h(rr.get('source',''))}</span>"
                    f"{button(rr.get('link',''), 'News', bg='#5f6368')}"
                    "</div></div>"
                )
            html.append("</div>")

    html.append(section_header("Playing Time Opportunities", "#f9ab00"))
    if not opp_alerts_weekly:
        html.append("<div style='color:#666;'>No positive playing-time signals detected for rostered players in the past 14 days.</div>")
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

    html.append(section_header("Major League Adds", "#0b8043"))
    if mlb_adds_df is None or mlb_adds_df.empty:
        html.append("<div style='color:#666;'>No MLB add candidates found in available pool (or MLB level couldn’t be detected).</div>")
    else:
        html.append(render_table_html(mlb_adds_df, "Top MLB Adds (Available) — max 10", html_cols={"Savant"}))

    html.append(section_header("Prospect Adds", "#5f6368"))
    if prospect_adds_df is None or prospect_adds_df.empty:
        html.append("<div style='color:#666;'>No prospect add candidates found in available pool after filters.</div>")
    else:
        html.append(render_table_html(prospect_adds_df, "Top Prospect Adds (Available) — max 10", html_cols={"Savant", "B-Ref"}))

    html.append("</body></html>")
    html_body = "".join(html)

    log(f"[weekly] about to send email: subject={subject} to={RECIPIENT} from={SENDER}")
    send_email(subject, text_body, html_body)
    log("[weekly] send_email() returned (no exception)")

    if env_flag("IS_SCHEDULED"):
        mark_weekly_sent(state)
    save_state(state)


# =========================
# Tests / modes
# =========================
def run_smtp_test() -> None:
    log("[test] running smtp test")
    send_email("SMTP Test", "If you received this, SMTP works.", "<b>If you received this, SMTP works.</b>")
    log("[test] smtp test sent")


def run_news_test() -> None:
    send_email("Daily Test", "Test email for daily.", "<b>Test email for daily.</b>")


def run_daily_realnews_test() -> None:
    run_daily(lookback_hours=24 * 14)


def run_weekly_test() -> None:
    run_weekly(force=True)


# =========================
# Main
# =========================
def main() -> None:
    log("[main] starting")
    ensure_state_files()
    mode = os.getenv("RUN_MODE", "daily").strip()
    validate_env(mode)
    startup_summary(mode)
    log(f"[main] RUN_MODE={mode}")

    if mode == "smtp_test":
        run_smtp_test()
    elif mode == "news_test":
        run_news_test()
    elif mode == "daily_realnews_test":
        run_daily_realnews_test()
    elif mode == "weekly_test":
        run_weekly_test()
    elif mode in ("spring_training_daily", "spring_daily_all"):
        run_spring_training_daily_allgames()
    elif mode == "daily":
        run_daily()
    elif mode == "weekly":
        run_weekly()
    else:
        raise SystemExit("Invalid RUN_MODE")


if __name__ == "__main__":
    try:
        log("[boot] entering __main__")
        main()
        log("[boot] main() finished")
    except Exception as e:
        import traceback
        log(f"[fatal] {type(e).__name__}: {e}")
        traceback.print_exc()
        raise

