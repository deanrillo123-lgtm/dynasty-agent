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
import requests

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
        with open(STATE_PATH, "w") as f:
            json.dump({
                "last_run_utc": None,
                "player_cache": {},
                "seen_rss_ids": [],
                "last_daily_local_date": None
            }, f, indent=2)

    for p in [WEEKLY_OFFICIAL_PATH, WEEKLY_REPORTS_PATH]:
        if not os.path.exists(p):
            with open(p, "w") as f:
                f.write("")

def load_state():
    ensure_state_files()
    with open(STATE_PATH) as f:
        return json.load(f)

def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)

def append_jsonl(path, record):
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")

def read_jsonl(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for line in f:
            try:
                out.append(json.loads(line.strip()))
            except:
                pass
    return sorted(out, key=lambda x: x.get("utc", ""))

def reset_jsonl(path):
    with open(path, "w") as f:
        f.write("")

# ----------------------------
# Email
# ----------------------------

def send_email(subject, body):
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

            team = row[idx_team].strip() if idx_team is not None else ""
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
    except:
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
    except:
        return []

def tx_since(tx_list, since):
    out = []
    for t in tx_list:
        d = t.get("date")
        if not d:
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        except:
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
    return hashlib.sha1(f"{source}|{title}|{link}".encode()).hexdigest()

def _build_name_patterns(roster_names):
    patterns = []
    for name in roster_names:
        patterns.append((name, re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)))
    return patterns

def _google_news_url(query):
    from urllib.parse import quote_plus
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"

def _build_google_news_sources(roster_names):
    sources = []
    chunk_size = 8
    for i in range(0, len(roster_names), chunk_size):
        chunk = roster_names[i:i+chunk_size]
        or_part = " OR ".join([f"\"{n}\"" for n in chunk])
        query = f"({or_part}) baseball (injury OR IL OR optioned OR promoted OR demoted OR trade)"
        sources.append({"name": f"Google News chunk {i//chunk_size+1}", "url": _google_news_url(query)})

    sources.append({"name": "Google News CBS fantasy", "url": _google_news_url("site:cbssports.com/fantasy baseball")})
    sources.append({"name": "Google News RotoBaller", "url": _google_news_url("site:rotoballer.com baseball")})

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
            entries = feed.entries[:80]
        except:
            continue

        for e in entries:
            title = _normalize(getattr(e, "title", ""))
            link = _normalize(getattr(e, "link", ""))
            summary = _normalize(getattr(e, "summary", ""))
            blob = f"{title} {summary}"

            cid = _content_id(src["name"], title, link)
            if cid in seen:
                continue

            pub_dt = now_utc()
            players = []

            for full, pat in patterns:
                if pat.search(blob):
                    players.append(full)

            if not players:
                continue

            for p in players:
                matched.append({
                    "utc": pub_dt.isoformat(),
                    "player": p,
                    "source": src["name"],
                    "title": title,
                    "link": link,
                    "cid": cid
                })

            seen.add(cid)

    state["seen_rss_ids"] = list(seen)[-8000:]
    return matched

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

def run_daily(lookback_hours=None):
    state = load_state()
    roster_df = load_roster()
    roster = roster_df["player_name"].tolist()

    print(f"[roster] players={len(roster)} sample={roster[:10]}")

    if os.getenv("IS_SCHEDULED") == "1":
        if not is_daily_time(state):
            print("Skipping - not 6am CT")
            save_state(state)
            return

    since = now_utc() - timedelta(hours=lookback_hours or 24)

    official = []
    for player in roster:
        pid = lookup_mlbam_id(player, state)
        if not pid:
            continue
        tx = tx_since(fetch_transactions(pid), since)
        for t in tx:
            official.append((player, t["desc"], t["utc"]))
            append_jsonl(WEEKLY_OFFICIAL_PATH, {"player": player, **t})

    reports = fetch_reports(roster, state)
    for r in reports:
        append_jsonl(WEEKLY_REPORTS_PATH, r)

    print(f"[daily] official_items={len(official)} reports_items={len(reports)}")

    if not official and not reports:
        if os.getenv("IS_SCHEDULED") == "1":
            mark_daily_sent(state)
        save_state(state)
        return

    body = ["# Dynasty Daily Update", ""]
    body.append("## Transaction Wire (Official)")
    body.extend([f"- **{p}** — {d}" for p,d,_ in official] or ["No official transactions."])
    body.append("")
    body.append("## Reports / Quotes")
    if reports:
        for r in reports:
            body.append(f"- **{r['player']}** ({r['source']}) — {r['title']} — {r['link']}")
    else:
        body.append("No matched reports.")
    body.append("")

    send_email(f"Dynasty Daily Update — {local_now().strftime('%b %d')}", "\n".join(body))

    if os.getenv("IS_SCHEDULED") == "1":
        mark_daily_sent(state)

    save_state(state)

# ----------------------------
# Weekly (stats + recap)
# ----------------------------

def run_weekly():
    # Weekly code omitted for brevity — unchanged from earlier stats version
    pass

# ----------------------------
# Test Modes
# ----------------------------

def run_smtp_test():
    send_email("SMTP Test", "If you received this, SMTP works.")

def run_news_test():
    send_email("Daily Test", "Test email for daily.")

def run_daily_realnews_test():
    run_daily(lookback_hours=24*14)

def main():
    ensure_state_files()
    mode = os.getenv("RUN_MODE", "daily")
    print(f"[main] RUN_MODE={mode}")

    if mode == "smtp_test":
        run_smtp_test()
    elif mode == "news_test":
        run_news_test()
    elif mode == "daily_realnews_test":
        run_daily_realnews_test()
    elif mode == "daily":
        run_daily()
    elif mode == "weekly":
        run_weekly()
    else:
        raise SystemExit("Invalid RUN_MODE")

if __name__ == "__main__":
    main()
