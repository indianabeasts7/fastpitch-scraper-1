# fastpitch_scraper.py
# Production-ready multi-site fastpitch tournament scraper using ScrapingAnt.
# - Requires environment variable: SCRAPINGANT_KEY
# - Writes fastpitch_master.json and fastpitch_master.csv
# - Defensive: retries, timeouts, fallbacks, never raises unhandled exceptions

import os
import json
import csv
import time
import random
import requests
from requests.utils import quote
from bs4 import BeautifulSoup

# -------------------------------
# Config
# -------------------------------
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY", "").strip()
USE_SCRAPINGANT = bool(SCRAPINGANT_KEY)

# Tuning
SCRAPINGANT_TIMEOUT = 40  # seconds per attempt
SCRAPINGANT_RETRIES = 3
DIRECT_TIMEOUT = 15
DIRECT_RETRIES = 2

# -------------------------------
# Helper: ScrapingAnt fetch (GET)
# -------------------------------
def fetch_via_scrapingant(target_url, timeout=SCRAPINGANT_TIMEOUT, max_retries=SCRAPINGANT_RETRIES):
    """
    Fetch through ScrapingAnt's general endpoint with render enabled.
    Returns response text or None.
    """
    if not SCRAPINGANT_KEY:
        print("fetch_via_scrapingant: missing SCRAPINGANT_KEY")
        return None

    quoted = quote(target_url, safe="")
    # Use HTTPS; render=true to allow JS-rendered pages.
    proxy_url = (
        f"https://api.scrapingant.com/v2/general?api_key={SCRAPINGANT_KEY}"
        f"&url={quoted}&render=true"
    )

    headers = {
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(proxy_url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"fetch_via_scrapingant: attempt {attempt} failed for {target_url}: {e}")
            if attempt < max_retries:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
    return None


# -------------------------------
# Helper: Direct fetch (no proxy) fallback
# -------------------------------
def fetch_direct(target_url, timeout=DIRECT_TIMEOUT, max_retries=DIRECT_RETRIES):
    headers = DEFAULT_HEADERS.copy()
    backoff = 0.8
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(target_url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"fetch_direct: attempt {attempt} failed for {target_url}: {e}")
            if attempt < max_retries:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
    return None


# -------------------------------
# Unified fetch: primary=ScrapingAnt, fallback=direct
# -------------------------------
def fetch(target_url, prefer_proxy=True):
    """
    Try proxy first (ScrapingAnt) if enabled, then fallback to direct.
    Returns text or None.
    """
    if USE_SCRAPINGANT and prefer_proxy:
        txt = fetch_via_scrapingant(target_url)
        if txt:
            return txt
        # fall through to direct fetch
    return fetch_direct(target_url)


# -------------------------------
# Normalizer
# -------------------------------
def normalize_event(event):
    return {
        "event_name": event.get("event_name") or event.get("name") or event.get("Name") or "N/A",
        "start_date": event.get("start_date") or event.get("startDate") or event.get("StartDate") or "N/A",
        "end_date": event.get("end_date") or event.get("endDate") or event.get("EndDate") or "N/A",
        "location": event.get("location") or event.get("Location") or event.get("city") or "N/A",
        "sanction": event.get("sanction") or event.get("source") or "N/A",
        "link": event.get("link") or event.get("url") or event.get("link_url") or "N/A",
        "age_divisions": event.get("age_divisions") or event.get("age") or []
    }


# -------------------------------
# USSSA (fastpitch) - uses the FASTPITCH endpoint
# -------------------------------
def scrape_usssa():
    print("Running scrape_usssa...")
    # This endpoint may return JSON or HTML depending on request; we proxy+render to be safe.
    endpoint = "https://usssa.com/api/tournaments/searchFastpitch"
    txt = fetch(endpoint, prefer_proxy=True)
    if not txt:
        print("USSSA error: no response from endpoint")
        return []

    # Try parse as JSON first
    try:
        raw = json.loads(txt)
    except Exception:
        # If not JSON, try to locate JSON blob inside HTML
        try:
            soup = BeautifulSoup(txt, "lxml")
            # common patterns: key named tournaments or Items
            # try to find script tags with JSON:
            for script in soup.find_all("script"):
                s = script.string
                if not s:
                    continue
                if "tournaments" in s or "Items" in s or "items" in s:
                    # try to extract JSON object from script
                    start = s.find("{")
                    end = s.rfind("}") + 1
                    if start != -1 and end != -1:
                        snippet = s[start:end]
                        try:
                            raw = json.loads(snippet)
                            break
                        except Exception:
                            continue
            else:
                raw = {}
        except Exception as e:
            print("USSSA parse error:", e)
            return []

    items = []
    if isinstance(raw, dict):
        items = raw.get("tournaments") or raw.get("Items") or raw.get("items") or raw.get("data") or []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    events = []
    for t in items:
        try:
            # be defensive about field names
            name = t.get("name") or t.get("Name") or t.get("tournamentName") or "N/A"
            start = t.get("startDate") or t.get("StartDate") or t.get("Start") or "N/A"
            end = t.get("endDate") or t.get("EndDate") or "N/A"
            city = t.get("city") or t.get("City") or ""
            state = t.get("state") or t.get("State") or ""
            tid = t.get("tournamentID") or t.get("TournamentID") or t.get("id") or ""
            link = f"https://usssa.com/tournament/{tid}" if tid else (t.get("link") or "N/A")
            events.append({
                "event_name": name,
                "start_date": start,
                "end_date": end,
                "location": ", ".join(p for p in [city, state] if p),
                "sanction": "USSSA",
                "link": link
            })
        except Exception as e:
            print("USSSA item parse error:", e)
            continue

    print(f"USSSA: found {len(events)} events")
    return events


# -------------------------------
# USFA
# -------------------------------
def scrape_usfa():
    print("Running scrape_usfa...")
    url = "https://usfastpitch.com/tournaments"
    txt = fetch(url, prefer_proxy=True)
    if not txt:
        print("USFA fetch failed")
        return []

    soup = BeautifulSoup(txt, "lxml")
    events = []

    # multiple selector attempts
    selectors = [
        ".tournament-card", ".card.tournament", ".event", ".tournaments-list .tournament", ".listing"
    ]
    found = []
    for sel in selectors:
        found = soup.select(sel)
        if found:
            break

    if not found:
        # fallback: search for anchor lists
        anchors = soup.select("a")
        for a in anchors[:200]:
            text = a.get_text(strip=True)
            if len(text) > 20 and ("tournament" in text.lower() or "classic" in text.lower()):
                href = a.get("href") or "N/A"
                events.append({
                    "event_name": text,
                    "start_date": "N/A",
                    "end_date": "N/A",
                    "location": "N/A",
                    "sanction": "USFA",
                    "link": href
                })
        print(f"USFA: fallback found {len(events)} events")
        return events

    for c in found:
        try:
            title = (c.select_one(".title") or c.select_one("h3") or c.select_one("a"))
            title_text = title.get_text(strip=True) if title else "N/A"
            date = (c.select_one(".date") or c.select_one(".dates") or c.select_one(".t-date"))
            date_text = date.get_text(strip=True) if date else "N/A"
            loc = (c.select_one(".location") or c.select_one(".place") or c.select_one(".t-location"))
            loc_text = loc.get_text(strip=True) if loc else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"
            events.append({
                "event_name": title_text or "N/A",
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "USFA",
                "link": link
            })
        except Exception:
            continue

    print(f"USFA: found {len(events)} events")
    return events


# -------------------------------
# PGF
# -------------------------------
def scrape_pgf():
    print("Running scrape_pgf...")
    url = "https://pgfusa.com/tournaments"
    txt = fetch(url, prefer_proxy=True)
    if not txt:
        print("PGF fetch failed")
        return []

    soup = BeautifulSoup(txt, "lxml")
    events = []

    # try a few known patterns
    cards = soup.select(".tourney-box, .tournament, .event-card, .t-list-item, .tourney")
    if not cards:
        cards = soup.select("article, .card, .listing")

    for c in cards:
        try:
            title_el = c.select_one(".t-title, h3, .title, a")
            title = title_el.get_text(strip=True) if title_el else "N/A"
            date_el = c.select_one(".t-date, .date")
            date_text = date_el.get_text(strip=True) if date_el else "N/A"
            loc_el = c.select_one(".t-loc, .location")
            loc_text = loc_el.get_text(strip=True) if loc_el else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"
            events.append({
                "event_name": title,
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "PGF",
                "link": link
            })
        except Exception:
            continue

    print(f"PGF: found {len(events)} events")
    return events


# -------------------------------
# Bullpen Tournaments
# -------------------------------
def scrape_bullpen():
    print("Running scrape_bullpen...")
    url = "https://play.bullpentournaments.com/events"
    txt = fetch(url, prefer_proxy=True)
    if not txt:
        print("Bullpen fetch failed")
        return []

    soup = BeautifulSoup(txt, "lxml")
    events = []

    rows = soup.select(".event-row, .event, .event-card")
    if not rows:
        rows = soup.select("table tr, .list-item, li")

    for row in rows:
        try:
            title_el = row.select_one(".name, h3, a")
            title = title_el.get_text(strip=True) if title_el else "N/A"
            date_el = row.select_one(".dates, .date")
            date_text = date_el.get_text(strip=True) if date_el else "N/A"
            loc_el = row.select_one(".location, .place")
            loc_text = loc_el.get_text(strip=True) if loc_el else "N/A"
            link_tag = row.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"

            events.append({
                "event_name": title,
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "Bullpen",
                "link": link
            })
        except Exception:
            continue

    print(f"Bullpen: found {len(events)} events")
    return events


# -------------------------------
# SoftballConnected
# -------------------------------
def scrape_softball_connected():
    print("Running scrape_softball_connected...")
    url = "https://softballconnected.com/tournaments"
    txt = fetch(url, prefer_proxy=True)
    if not txt:
        print("SoftballConnected fetch failed")
        return []

    soup = BeautifulSoup(txt, "lxml")
    events = []

    cards = soup.select(".tournament-card, .card, .tournament, .listing")
    for c in cards:
        try:
            title_el = c.select_one(".title, h3, .name, a")
            title = title_el.get_text(strip=True) if title_el else "N/A"
            date_el = c.select_one(".date, .dates")
            date_text = date_el.get_text(strip=True) if date_el else "N/A"
            loc_el = c.select_one(".location, .place, .city")
            loc_text = loc_el.get_text(strip=True) if loc_el else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"

            events.append({
                "event_name": title,
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "SoftballConnected",
                "link": link
            })
        except Exception:
            continue

    print(f"SoftballConnected: found {len(events)} events")
    return events


# -------------------------------
# Controller
# -------------------------------
def run_all_scrapers():
    scrapers = [
        scrape_usssa,
        scrape_usfa,
        scrape_pgf,
        scrape_bullpen,
        scrape_softball_connected,
    ]

    all_events = []
    for scraper in scrapers:
        try:
            print(f"Running {scraper.__name__}...")
            data = scraper()
            if data:
                for e in data:
                    safe = normalize_event(e)
                    all_events.append(safe)
        except Exception as ex:
            print(f"{scraper.__name__} FAILED: {ex}")
        # polite pause
        time.sleep(random.uniform(0.5, 1.2))

    # Write JSON
    try:
        with open("fastpitch_master.json", "w", encoding="utf-8") as f:
            json.dump(all_events, f, indent=2)
    except Exception as e:
        print("Error writing JSON:", e)

    # Write CSV
    try:
        with open("fastpitch_master.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["event_name", "start_date", "end_date", "location", "sanction", "link"])
            for e in all_events:
                writer.writerow([
                    e.get("event_name", "N/A"),
                    e.get("start_date", "N/A"),
                    e.get("end_date", "N/A"),
                    e.get("location", "N/A"),
                    e.get("sanction", "N/A"),
                    e.get("link", "N/A"),
                ])
    except Exception as e:
        print("Error writing CSV:", e)

    print(f"Scraped {len(all_events)} events.")
    return all_events


# -------------------------------
# API helper for server.py
# -------------------------------
def get_events():
    """
    Returns the JSON object from fastpitch_master.json if present, else an empty object.
    """
    try:
        with open("fastpitch_master.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            # Keep the legacy wrapper structure if old code expects it
            return {"count": len(data), "events": data}
    except Exception:
        return {"count": 0, "events": []}


# -------------------------------
# If run as script, run once
# -------------------------------
if __name__ == "__main__":
    ev = run_all_scrapers()
    print("Done. Events:", len(ev))
