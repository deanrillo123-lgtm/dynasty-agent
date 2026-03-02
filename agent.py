import os
import json
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
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
    # Fantrax exports sometimes have commas inside quoted fields or inconsistent columns.
    # Use python engine for more tolerant parsing, and fall back to skipping bad lines.
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

    # Drop obvious header repeats / garbage rows
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


def build_weekly_email_body() -> str:
    roster = load_roster()
    news = read_weekly_news()

    body = []
    body.append(f"# Dynasty Weekly Report — {local_now().strftime('%b %d, %Y')}")
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
    body.append("## Season-to-date MLB Stats (FanGraphs tables)")
    body.append("_Note: This section is MLB-only; MiLB advanced metrics are often not available._")
    body.append("")

    year = local_now().year
    names = set(roster["player_name"].tolist())

    try:
        bat = batting_stats(year)
        pit = pitching_stats(year)

        bat = bat[bat["Name"].isin(names)].copy()
        pit = pit[pit["Name"].isin(names)].copy()

        if not bat.empty:
            cols = ["Name", "Team", "G", "H", "HR", "RBI", "SB", "AVG", "OBP", "wRC+", "K%", "BB%"]
            show = bat[[c for c in cols if c in bat.columns]].sort_values("Name")
            body.append("### Hitters (Season)")
            body.append(show.to_markdown(index=False))
            body.append("")
        else:
            body.append("### Hitters (Season)")
            body.append("No MLB hitter matches found.")
            body.append("")

        if not pit.empty:
            cols = ["Name", "Team", "GS", "IP", "ERA", "FIP", "K%", "BB%"]
            show = pit[[c for c in cols if c in pit.columns]].sort_values("Name")
            body.append("### Pitchers (Season)")
            body.append(show.to_markdown(index=False))
            body.append("")
        else:
            body.append("### Pitchers (Season)")
            body.append("No MLB pitcher matches found.")
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
    body = build_weekly_email_body()
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
    main()
