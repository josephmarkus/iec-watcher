#!/usr/bin/env python3
"""Scrape the IRCC IEC pool page for one country/category, store a snapshot in
SQLite, and push ntfy.sh notifications for daily digests, detected changes,
season rollovers, and scrape failures.

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

# Every notification shares this prefix so the alerts group together and read
# consistently. Notifications are deliberately emoji-free, which is also why no
# ntfy "Tags" header is sent — ntfy renders known tags as emoji in the title.
TITLE_PREFIX = "Canada"

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

DIFF_FIELDS = [
    ("quota", "Quota"),
    ("spots_available", "Spots available"),
    ("candidates_in_pool", "Candidates in pool"),
    ("invitations_issued", "Invited to date"),
    ("first_round_text", "First round"),
    ("final_round_text", "Final round"),
    ("chances_code", "Chance rating"),
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
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
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


def send_ntfy(config, title, body, priority="default", click=None):
    url = f"{config['ntfy_base_url'].rstrip('/')}/{config['ntfy_topic']}"
    headers = {"Title": title.encode("utf-8"), "Priority": priority.encode("utf-8")}
    if click:
        headers["Click"] = click.encode("utf-8")

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


def build_digest_message(current):
    return "\n".join(
        [
            f"{current['data_as_of_text']}",
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
    previous = get_last_ok_snapshot(conn)

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
            click=config["target_page_url"],
        )

    prev_status = previous["pool_status"] if previous is not None else None
    if prev_status is not None and prev_status != current["pool_status"]:
        notify(
            f"{TITLE_PREFIX} — pool status changed",
            f"Pool status: {prev_status} → {current['pool_status']}",
            priority="high",
            click=config["target_page_url"],
        )

    diffs = compute_diffs(previous, current)
    if diffs:
        body = "\n".join(f"{label}: {format_int(prev) if isinstance(prev, int) else prev} → "
                          f"{format_int(cur) if isinstance(cur, int) else cur}" for label, prev, cur in diffs)
        notify(
            f"{TITLE_PREFIX} — change detected",
            body,
            priority="high",
            click=config["target_page_url"],
        )

    today_str = scraped_at[:10]
    if not digest_already_sent_today(conn, today_str):
        sent = notify(
            f"{TITLE_PREFIX} — daily",
            build_digest_message(current),
            priority="default",
            click=config["target_page_url"],
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

    def notify(title, body, priority="default", click=None):
        if args.dry_run:
            log.info("[DRY RUN] %s | %s", title, body.replace("\n", " / "))
            return False
        return send_ntfy(config, title, body, priority=priority, click=click)

    ok = run(config, conn, notify)
    conn.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
