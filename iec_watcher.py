#!/usr/bin/env python3
"""Scrape the IRCC IEC pool page for one country/category, store a snapshot in
SQLite, and push ntfy.sh notifications for new data publications, detected
changes, season rollovers, and scrape failures.

No third-party dependencies — stdlib only, so it can run under the system
python3 with no venv/pip setup (important for an unattended launchd job).
"""
import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "db" / "iec.db"
LOG_PATH = BASE_DIR / "logs" / "iec-watcher.log"

HTML_URL_TEMPLATE = "https://ircc.canada.ca/english/work/iec/selections.asp?country={country}&cat={category}"
XML_URL = "https://ircc.canada.ca/english/work/iec/selections.xml"

USER_AGENT = "iec-watcher/1.0 (personal pool-status monitor)"

# no-cache asks the CDN in front of ircc.canada.ca to revalidate against the
# origin rather than hand back whatever its edge node last cached. Observed
# 2026-07-24: a fetch 9 minutes AFTER the XML's own Last-Modified still served
# the previous week's copy, which cost a full day's notice. Pragma is the
# HTTP/1.0 spelling, sent for any intermediary that ignores Cache-Control.
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Every notification shares this prefix so the alerts group together and read
# consistently. Notifications are deliberately emoji-free, which is also why no
# ntfy "Tags" header is sent — ntfy renders known tags as emoji in the title.
TITLE_PREFIX = "Canada"

# No ntfy "Click" header is sent anywhere in this script, deliberately. With no
# click action, tapping a notification opens the ntfy app on the topic, where
# the full history of alerts is; a Click URL would instead hand off to the
# browser and lose that. Don't add one back without wanting that trade.

CHANCES_LABELS = {
    0: "Not applicable",
    1: "Excellent",
    2: "Very good",
    3: "Fair",
    4: "Low",
    5: "Very low",
}

RETRY_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 15

# Same fields, order and labels as the daily digest, so the two notifications
# read alike. chances_label rather than chances_code: the label is 1:1 with the
# code, so detection is unchanged, but the alert reads "Very low → Low" instead
# of "5 → 4".
DIFF_FIELDS = [
    ("chances_label", "Chance"),
    ("spots_available", "Spots available"),
    ("candidates_in_pool", "Candidates in pool"),
    ("quota", "Quota"),
    ("invitations_issued", "Invited to date"),
    ("first_round_text", "First round"),
    ("final_round_text", "Final round"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("iec_watcher")


def load_config():
    with CONFIG_PATH.open() as f:
        return json.load(f)


def fetch(url, retries=RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS):
    """GET a URL with retries, returning (body_bytes, headers_dict).

    Retries absorb the few seconds of Wi-Fi reassociation right after the
    Mac wakes from sleep.
    """
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=REQUEST_HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read(), dict(resp.headers)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
            log.warning("Fetch attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def parse_html(html_bytes):
    html = html_bytes.decode("utf-8", errors="replace")

    status_match = re.search(r"This pool is (open|closed)\.", html)
    pool_status = status_match.group(1) if status_match else "unknown"

    banner_match = re.search(r"<p>(The pools for the (\d{4}) season.*?)</p>", html, re.DOTALL)
    if banner_match:
        season_year = int(banner_match.group(2))
        season_banner_text = re.sub(r"<[^>]+>", "", banner_match.group(1))
        season_banner_text = re.sub(r"\s+", " ", season_banner_text).strip()
    else:
        season_year = None
        season_banner_text = None

    modified_match = re.search(r'<time property="dateModified">([\d-]+)</time>', html)
    page_date_modified = modified_match.group(1) if modified_match else None

    return {
        "pool_status": pool_status,
        "season_year": season_year,
        "season_banner_text": season_banner_text,
        "page_date_modified": page_date_modified,
    }


def parse_xml(xml_bytes, headers, country_code, category_code):
    root = ET.fromstring(xml_bytes)

    chancesdate_el = root.find("chancesdate")
    data_as_of_text = (
        chancesdate_el.text.strip() if chancesdate_el is not None and chancesdate_el.text else None
    )

    entry = None
    for country_el in root.findall("country"):
        if country_el.get("code") == country_code and country_el.get("category") == category_code:
            entry = country_el
            break
    if entry is None:
        raise RuntimeError(f"No XML entry found for country={country_code!r} category={category_code!r}")

    def text_of(tag):
        el = entry.find(tag)
        return el.text.strip() if el is not None and el.text else None

    def int_of(tag):
        raw = text_of(tag)
        return int(raw.replace(",", "")) if raw is not None else None

    chances_code = int_of("chances")

    xml_last_modified_utc = None
    last_modified_raw = headers.get("Last-Modified")
    if last_modified_raw:
        try:
            xml_last_modified_utc = parsedate_to_datetime(last_modified_raw).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            xml_last_modified_utc = last_modified_raw

    return {
        "quota": int_of("quota"),
        "first_round_text": text_of("first"),
        "final_round_text": text_of("second"),
        "invitations_issued": int_of("invitations"),
        "candidates_in_pool": int_of("candidates"),
        "spots_available": int_of("spots"),
        "chances_code": chances_code,
        "chances_label": CHANCES_LABELS.get(chances_code, "Unknown") if chances_code is not None else None,
        "data_as_of_text": data_as_of_text,
        "xml_last_modified_utc": xml_last_modified_utc,
    }


def init_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at_utc         TEXT NOT NULL,
            country_code           TEXT NOT NULL,
            category_code          TEXT NOT NULL,
            season_banner_text     TEXT,
            season_year            INTEGER,
            pool_status            TEXT,
            quota                  INTEGER,
            first_round_text       TEXT,
            final_round_text       TEXT,
            invitations_issued     INTEGER,
            spots_available        INTEGER,
            candidates_in_pool     INTEGER,
            chances_code           INTEGER,
            chances_label          TEXT,
            data_as_of_text        TEXT,
            xml_last_modified_utc  TEXT,
            page_date_modified     TEXT,
            ok                     INTEGER NOT NULL DEFAULT 1,
            error_message          TEXT,
            digest_sent            INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_scraped_at ON snapshots(scraped_at_utc)")
    conn.commit()


def get_last_ok_snapshot(conn):
    return conn.execute("SELECT * FROM snapshots WHERE ok = 1 ORDER BY id DESC LIMIT 1").fetchone()


def parse_data_as_of(text):
    """Parse IRCC's "August 07, 2026" stamp into a date, or None if unparseable."""
    if not text:
        return None
    try:
        return datetime.strptime(text.strip(), "%B %d, %Y").date()
    except ValueError:
        log.warning("Could not parse data_as_of_text %r", text)
        return None


def get_baseline_snapshot(conn, limit=90):
    """Return the stored snapshot carrying the freshest IRCC dataset.

    Not simply the newest row: IRCC intermittently serves a stale XML (observed
    2026-07-28/29, when the July 24 dataset reverted to July 17 for two days).
    Diffing against the last row would then report the regression, and report
    the re-advance again once the stale copy cleared — two alerts, no real news.
    Diffing against the freshest dataset seen so far reports neither.

    Ties keep the most recently scraped row. Rows with an unparseable stamp are
    skipped; if none can be parsed we fall back to the newest row.
    """
    rows = conn.execute(
        "SELECT * FROM snapshots WHERE ok = 1 ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    best, best_date = None, None
    for row in rows:
        row_date = parse_data_as_of(row["data_as_of_text"])
        if row_date is not None and (best_date is None or row_date > best_date):
            best, best_date = row, row_date
    if best is not None:
        return best
    return rows[0] if rows else None


def digest_already_sent_today(conn, today_str):
    row = conn.execute(
        "SELECT 1 FROM snapshots WHERE ok = 1 AND digest_sent = 1 AND substr(scraped_at_utc, 1, 10) = ? LIMIT 1",
        (today_str,),
    ).fetchone()
    return row is not None


def insert_snapshot(conn, data):
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cur = conn.execute(f"INSERT INTO snapshots ({columns}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    return cur.lastrowid


def mark_digest_sent(conn, row_id):
    conn.execute("UPDATE snapshots SET digest_sent = 1 WHERE id = ?", (row_id,))
    conn.commit()


def send_ntfy(config, title, body, priority="default"):
    url = f"{config['ntfy_base_url'].rstrip('/')}/{config['ntfy_topic']}"
    headers = {"Title": title.encode("utf-8"), "Priority": priority.encode("utf-8")}

    req = urllib.request.Request(url, data=body.encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        log.info("Sent ntfy notification: %s", title)
        return True
    except urllib.error.URLError as exc:
        log.error("Failed to send ntfy notification %r: %s", title, exc)
        return False


def format_int(n):
    return f"{n:,}" if isinstance(n, int) else "?"


def format_data_stamp(current):
    """The dataset's stamp, rendered with time of day and zone.

    IRCC's own <chancesdate> stamp is date-only ("August 21, 2026"), which
    can't separate two publications on the same day and hides how fresh the
    figures actually are. The XML's HTTP Last-Modified carries the publication
    time, so it is preferred, converted to whatever zone this machine is in so
    it reads against your own clock. The date-only stamp stays as the fallback
    for when the header is missing or unparseable.
    """
    raw = current.get("xml_last_modified_utc")
    if raw:
        try:
            local = datetime.fromisoformat(raw).astimezone()
        except ValueError:
            log.warning("Could not parse xml_last_modified_utc %r", raw)
        else:
            return f"{local.day} {local:%B %Y, %H:%M %Z}"
    return current["data_as_of_text"]


def is_unchanged(baseline, current):
    """True when this snapshot carries the same IRCC dataset as the baseline.

    Requires the as-of stamp to match as well as the figures: IRCC restating the
    same numbers under a newer stamp is news (it means a fresh publication that
    happened to move nothing), so that case still gets the full listing.
    """
    if baseline is None:
        return False
    if baseline["data_as_of_text"] != current["data_as_of_text"]:
        return False
    if baseline["pool_status"] != current["pool_status"]:
        return False
    return not compute_diffs(baseline, current)


def build_digest_message(current):
    """The full field listing for a freshly published IRCC dataset.

    Only sent when the data actually moved — see run(). IRCC refreshes this
    dataset weekly, so a genuinely daily digest repeated the same figures six
    days out of seven, which trains you to swipe the notification away unread,
    exactly when a real change needs to catch your eye. Unchanged days are
    still scraped and still stored; they just no longer notify.
    """
    return "\n".join(
        [
            format_data_stamp(current),
            f"Status: {current['pool_status']}",
            f"Chance: {current['chances_label']}",
            f"Spots available: {format_int(current['spots_available'])}",
            f"Candidates in pool: {format_int(current['candidates_in_pool'])}",
            f"Quota: {format_int(current['quota'])}",
            f"Invited to date: {format_int(current['invitations_issued'])}",
            f"First round: {current['first_round_text']}",
            f"Final round: {current['final_round_text']}",
        ]
    )


def build_change_message(current, diffs):
    def render(value):
        return format_int(value) if isinstance(value, int) else value

    return "\n".join(
        [format_data_stamp(current)]
        + [f"{label}: {render(prev)} → {render(cur)}" for label, prev, cur in diffs]
    )


def compute_diffs(previous, current):
    if previous is None:
        return []
    diffs = []
    for field, label in DIFF_FIELDS:
        prev_val = previous[field]
        cur_val = current[field]
        if prev_val != cur_val:
            diffs.append((label, prev_val, cur_val))
    return diffs


def run(config, conn, notify):
    scraped_at = datetime.now(timezone.utc).isoformat()
    # previous drives the HTML-derived comparisons (season banner, pool status);
    # baseline drives the XML-derived figure diffs. They are the same row unless
    # IRCC is currently serving a stale XML.
    previous = get_last_ok_snapshot(conn)
    baseline = get_baseline_snapshot(conn)

    try:
        html_bytes, _ = fetch(
            HTML_URL_TEMPLATE.format(country=config["country_code"], category=config["category_code"])
        )
        html_data = parse_html(html_bytes)

        xml_bytes, xml_headers = fetch(XML_URL)
        xml_data = parse_xml(xml_bytes, xml_headers, config["country_code"], config["category_code"])
    except Exception as exc:
        log.exception("Scrape failed")
        insert_snapshot(
            conn,
            {
                "scraped_at_utc": scraped_at,
                "country_code": config["country_code"],
                "category_code": config["category_code"],
                "ok": 0,
                "error_message": str(exc),
                "digest_sent": 0,
            },
        )
        notify(
            f"{TITLE_PREFIX} — watcher error",
            f"Scrape failed: {exc}\nCheck {LOG_PATH}",
            priority="high",
        )
        return False

    current = {
        "scraped_at_utc": scraped_at,
        "country_code": config["country_code"],
        "category_code": config["category_code"],
        "ok": 1,
        "error_message": None,
        "digest_sent": 0,
        **html_data,
        **xml_data,
    }
    row_id = insert_snapshot(conn, current)
    log.info("Inserted snapshot id=%d ok=1", row_id)

    prev_year = previous["season_year"] if previous is not None else None
    if prev_year is not None and current["season_year"] is not None and current["season_year"] > prev_year:
        notify(
            f"{TITLE_PREFIX} — {current['season_year']} season may be open",
            f"Banner changed from \"{prev_year}\" to \"{current['season_year']}\":\n"
            f"{current['season_banner_text']}\nGo check now.",
            priority="urgent",
        )

    prev_status = previous["pool_status"] if previous is not None else None
    if prev_status is not None and prev_status != current["pool_status"]:
        notify(
            f"{TITLE_PREFIX} — pool status changed",
            f"Status: {prev_status} → {current['pool_status']}",
            priority="high",
        )

    current_date = parse_data_as_of(current["data_as_of_text"])
    baseline_date = parse_data_as_of(baseline["data_as_of_text"]) if baseline is not None else None
    stale = current_date is not None and baseline_date is not None and current_date < baseline_date
    if stale:
        log.warning(
            "Stale IRCC data: served %s, already have %s — suppressing change alert",
            current_date,
            baseline_date,
        )

    diffs = [] if stale else compute_diffs(baseline, current)
    if diffs:
        notify(
            f"{TITLE_PREFIX} — change detected",
            build_change_message(current, diffs),
            priority="high",
        )

    # The listing goes out only when IRCC has actually published something new.
    # `stale` is included because a stale serve reaches this point looking like
    # a change (an older as-of stamp differs from the baseline's), and notifying
    # on it would announce last week's figures as fresh news.
    today_str = scraped_at[:10]
    if stale or is_unchanged(baseline, current):
        log.info(
            "No new IRCC data (as of %s) — notification suppressed, snapshot still stored",
            current["data_as_of_text"],
        )
    elif not digest_already_sent_today(conn, today_str):
        sent = notify(
            f"{TITLE_PREFIX} — data updated",
            build_digest_message(current),
            priority="default",
        )
        if sent:
            mark_digest_sent(conn, row_id)

    return True


def main():
    parser = argparse.ArgumentParser(description="Scrape IEC pool data and notify via ntfy.sh")
    parser.add_argument("--once", action="store_true", help="Run a single check (default behaviour)")
    parser.add_argument("--dry-run", action="store_true", help="Log notifications instead of sending them")
    args = parser.parse_args()

    config = load_config()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    def notify(title, body, priority="default"):
        if args.dry_run:
            log.info("[DRY RUN] %s | %s", title, body.replace("\n", " / "))
            return False
        return send_ntfy(config, title, body, priority=priority)

    ok = run(config, conn, notify)
    conn.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
