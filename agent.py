# ==============================
# Dynasty Baseball Agent
# ==============================

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

# ==============================
# CONFIG
# ==============================

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

# ==============================
# NEWS SOURCES
# ==============================

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

# ==============================
# STATE MANAGEMENT
# ==============================

def ensure_state_files():
    os.makedirs(STATE_DIR, exist_ok=True)

    if not os.path.exists(STATE_PATH):
        with open(STATE_PATH, "w") as f:
            json.dump({
                "seen_rss_ids": [],
                "player_cache": {}
            }, f)

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
                out.append(json.loads(line))
            except:
                pass
    return out


# ==============================
# EMAIL
# ==============================

def send_email(subject, text, html):

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Dynasty Agent <{SENDER}>"
    msg["To"] = RECIPIENT

    msg.set_content(text)
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER, SENDER_PW)
        server.send_message(msg)


# ==============================
# ROSTER
# ==============================

def load_roster():

    players = []
    teams = []

    idx_player = None
    idx_team = None

    with open(ROSTER_PATH) as f:
        rdr = csv.reader(f)

        for row in rdr:

            if "Player" in row and "Team" in row:
                idx_player = row.index("Player")
                idx_team = row.index("Team")
                continue

            if idx_player is None:
                continue

            if len(row) <= idx_player:
                continue

            name = row[idx_player].strip()
            team = row[idx_team].strip()

            if not name or name.lower() == "player":
                continue

            name = re.sub(r"\s*\(.*\)$", "", name)

            players.append(name)
            teams.append(team)

    df = pd.DataFrame({"player_name": players, "team_abbrev": teams})

    return df.drop_duplicates(subset=["player_name"]).reset_index(drop=True)


# ==============================
# MLB PLAYER ID
# ==============================

def lookup_mlbam_id(name, state):

    cache = state["player_cache"]

    if name in cache:
        return cache[name]

    try:
        res = statsapi.lookup_player(name)

        if not res:
            return None

        pid = int(res[0]["id"])

        cache[name] = pid

        return pid

    except:
        return None


# ==============================
# RSS COLLECTION
# ==============================

def _normalize(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def _content_id(source, title, link):
    return hashlib.sha1(f"{source}|{title}|{link}".encode()).hexdigest()


def fetch_reports(roster_names, state):

    sources = [
        ("MLB.com", MLB_NEWS_FEED),
        ("MLB Pipeline", MLB_PIPELINE_RSS),
        ("MiLB", MILB_NEWS_RSS),
        ("FanGraphs", FANGRAPHS_RSS),
        ("FanGraphs Prospects", FANGRAPHS_PROSPECTS_RSS),
        ("Baseball America", BASEBALL_AMERICA_RSS),
        ("Baseball Prospectus", BASEBALL_PROSPECTUS_RSS),
        ("MLBTR", MLBTR_MAIN_FEED),
        ("MLBTR Transactions", MLBTR_TX_FEED),
        ("CBS", CBS_MLB_RSS),
    ]

    seen = set(state["seen_rss_ids"])

    reports = []

    for name, url in sources:

        try:
            feed = feedparser.parse(url)
            entries = feed.entries[:60]
        except:
            continue

        for e in entries:

            title = _normalize(getattr(e, "title", ""))
            link = _normalize(getattr(e, "link", ""))
            summary = _normalize(getattr(e, "summary", ""))

            blob = f"{title} {summary}"

            cid = _content_id(name, title, link)

            if cid in seen:
                continue

            for player in roster_names:

                if re.search(rf"\b{re.escape(player)}\b", blob, re.I):

                    reports.append({
                        "player": player,
                        "title": title,
                        "link": link,
                        "source": name,
                        "cid": cid
                    })

                    seen.add(cid)

    state["seen_rss_ids"] = list(seen)[-8000:]

    return reports


# ==============================
# DAILY EMAIL
# ==============================

def run_daily():

    state = load_state()

    roster_df = load_roster()

    roster = roster_df["player_name"].tolist()

    reports = fetch_reports(roster, state)

    if not reports:
        save_state(state)
        return

    html = """
    <h2>Dynasty Daily Update</h2>
    """

    text = "Dynasty Daily Update\n\n"

    for r in reports:

        html += f"""
        <p><b>{r['player']}</b><br>
        {r['title']}<br>
        <a href="{r['link']}">News</a> ({r['source']})
        </p>
        """

        text += f"{r['player']} — {r['title']} ({r['source']}) {r['link']}\n"

    send_email(
        f"Dynasty Daily Update — {datetime.now().strftime('%b %d')}",
        text,
        html
    )

    save_state(state)


# ==============================
# WEEKLY EMAIL
# ==============================

def run_weekly():

    roster_df = load_roster()

    roster = roster_df["player_name"].tolist()

    html = "<h2>Dynasty Weekly Report</h2>"

    text = "Dynasty Weekly Report\n\n"

    # weekly stats placeholder

    html += "<p>Weekly report coming here with stats tables, hot performances, injuries and news recap.</p>"

    send_email(
        f"Dynasty Weekly Report — {datetime.now().strftime('%b %d')}",
        text,
        html
    )


# ==============================
# MAIN
# ==============================

def main():

    ensure_state_files()

    mode = os.getenv("RUN_MODE", "daily")

    if mode == "daily":
        run_daily()

    elif mode == "weekly":
        run_weekly()

    elif mode == "daily_realnews_test":
        run_daily()

    elif mode == "weekly_test":
        run_weekly()

    else:
        print("Invalid RUN_MODE")


if __name__ == "__main__":
    main()
