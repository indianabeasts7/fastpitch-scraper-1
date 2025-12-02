# fastpitch_scraper.py
# Full multi-site scraper that proxies requests through ScraperAPI (HTTPS),
# provides robust retries, safe normalization, and writes JSON+CSV output.
#
# Required env var: SCRAPERAPI_KEY
# Dependencies: requests, beautifulsoup4, lxml (already in requirements.txt)

import os
import json
import csv
import time
import random
import requests
from requests.utils import quote
from bs4 import BeautifulSoup

# -------------------------------
# Global defaults
# -------------------------------
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

# If you want to disable proxying and use direct fetches, set this False.
# For production keep True so sites with cloudfire/waf are proxied.
USE_SCRAPERAPI = True if SCRAPERAPI_KEY else False


# -------------------------------
# Helper: fetch via ScraperAPI (with retries)
# -------------------------------
def fetch_via_scraperapi(target_url, timeout=30, max_retries=3):
    """
    Fetch a URL through ScraperAPI (residential proxy) and return response text.
    Returns None on failure. Uses HTTPS endpoint (required).
    """
    if not SCRAPERAPI_KEY:
        print("fetch_via_scraperapi: Missing SCRAPERAPI_KEY (proxy disabled).")
        return None

    # Build scraperapi URL. Include country=us for better results and render options.
    quoted = quote(target_url, safe="")
    proxy_url = (
        f"https://api.scraperapi.com?api_key={SCRAPERAPI_KEY}"
        f"&url={quoted}"
        "&country=us"
        "&render=true"
        "&keep_headers=true"
    )

    headers = {
        "User-Agent": DEFAULT_HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    attempt = 0
    backoff = 1.0
    while attempt < max_retries:
        try:
            r = requests.get(proxy_url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            attempt += 1
            print(f"fetch_via_scraperapi: attempt {attempt} failed for {target_url}: {e}")
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2
    return None


def fetch_direct(target_url, timeout=20, max_retries=2):
    """
    Direct GET request (no proxy). Returns text or None.
    """
    headers = DEFAULT_HEADERS
    attempt = 0
    backoff = 0.8
    while attempt < max_retries:
        try:
            r = requests.get(target_url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            attempt += 1
            print(f"fetch_direct: attempt {attempt} failed for {target_url}: {e}")
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2
    return None


def fetch(target_url, prefer_proxy=True):
    """
    Unified fetch: tries ScraperAPI first (if enabled and prefer_proxy),
    then falls back to direct fetch. Returns text or None.
    """
    if USE_SCRAPERAPI and prefer_proxy:
        txt = fetch_via_scraperapi(target_url)
        if txt:
            return txt
        # fallback to direct if proxy failed
    return fetch_direct(target_url)


# -------------------------------
# Normalization helper
# -------------------------------
def normalize_event(event):
    # Accept different field names and map to canonical ones.
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
# USSSA scraper (uses dedicated FASTPITCH endpoint via proxy)
# -------------------------------
def scrape_usssa():
    print("Running scrape_usssa...")
    # USSSA has a fastpitch-specific endpoint used by the site; we'll proxy it
    api_endpoint = "https://usssa.com/api/tournaments/searchFastpitch"

    # Use proxy; prefer proxy mode (USSSA blocks datacenter IPs)
    txt = fetch(api_endpoint, prefer_proxy=True)
    if not txt:
        print("USSSA error: no response from endpoint")
        return []

    # Try JSON parse first
    try:
        raw = json.loads(txt)
    except Exception:
        # If proxy returned HTML (sometimes), attempt to find JSON-like within
        try:
            soup = BeautifulSoup(txt, "lxml")
            # sometimes endpoint returns a script tag with initial data
            script = soup.find("script", string=lambda s: s and "tournaments" in s)
            if script:
                # attempt to extract JSON substring
                text = script.string
                start = text.find("{")
                end = text.rfind("}") + 1
                raw = json.loads(text[start:end])
            else:
                raw = {}
        except Exception as e:
            print("USSSA parse error:", e)
            return []

    # Raw should contain "tournaments" or "Items"
    items = []
    if isinstance(raw, dict):
        items = raw.get("tournaments") or raw.get("Items") or raw.get("items") or []
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    events = []
    for t in items:
        try:
            name = t.get("name") or t.get("Name") or t.get("TournamentName") or "N/A"
            start = t.get("startDate") or t.get("StartDate") or t.get("Start") or "N/A"
            end = t.get("endDate") or t.get("EndDate") or "N/A"
            city = t.get("city") or t.get("City") or ""
            state = t.get("state") or t.get("State") or ""
            tid = t.get("tournamentID") or t.get("TournamentID") or t.get("id") or ""
            link = f"https://usssa.com/tournament/{tid}" if tid else t.get("link") or "N/A"

            events.append({
                "event_name": name,
                "start_date": start,
                "end_date": end,
                "location": f"{city}, {state}".strip(", "),
                "sanction": "USSSA",
                "link": link
            })
        except Exception as e:
            print("USSSA item parse error:", e)
            continue

    print(f"USSSA: found {len(events)} events")
    return events


# -------------------------------
# USFA scraper
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

    # Try a few common selectors
    selectors = [
        ".tournament-card",
        ".card.tournament",
        ".event",
        ".tournaments-list .tournament"
    ]
    found = []
    for sel in selectors:
        found = soup.select(sel)
        if found:
            break

    for c in found:
        try:
            title = (c.select_one(".title") or c.select_one("h3") or c.select_one("a")).get_text(strip=True) if c else ""
            date = (c.select_one(".date") or c.select_one(".dates") or c.select_one(".t-date"))
            date_text = date.get_text(strip=True) if date else "N/A"
            loc = (c.select_one(".location") or c.select_one(".place") or c.select_one(".t-location"))
            loc_text = loc.get_text(strip=True) if loc else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"
            events.append({
                "event_name": title or "N/A",
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "USFA",
                "link": link
            })
        except Exception as e:
            # continue on parse errors
            continue

    print(f"USFA: found {len(events)} events")
    return events


# -------------------------------
# PGF scraper
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

    # Try common card selectors
    cards = soup.select(".tourney-box, .tournament, .event-card, .t-list-item")
    for c in cards:
        try:
            title = (c.select_one(".t-title") or c.select_one("h3") or c.select_one(".title"))
            title_text = title.get_text(strip=True) if title else "N/A"
            date = (c.select_one(".t-date") or c.select_one(".date"))
            date_text = date.get_text(strip=True) if date else "N/A"
            loc = (c.select_one(".t-loc") or c.select_one(".location"))
            loc_text = loc.get_text(strip=True) if loc else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"

            events.append({
                "event_name": title_text,
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
# Bullpen Tournaments scraper
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
        # maybe it's presented in table
        rows = soup.select("table tr")

    for row in rows:
        try:
            title = (row.select_one(".name") or row.select_one("h3") or row.select_one("td a"))
            title_text = title.get_text(strip=True) if title else "N/A"
            date = (row.select_one(".dates") or row.select_one(".date") or row.select_one("td.date"))
            date_text = date.get_text(strip=True) if date else "N/A"
            loc = (row.select_one(".location") or row.select_one("td.location"))
            loc_text = loc.get_text(strip=True) if loc else "N/A"
            link_tag = row.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"

            events.append({
                "event_name": title_text,
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
# SoftballConnected scraper
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

    cards = soup.select(".tournament-card, .card, .tournament")
    for c in cards:
        try:
            title = (c.select_one(".title") or c.select_one("h3") or c.select_one(".name"))
            title_text = title.get_text(strip=True) if title else "N/A"
            date = (c.select_one(".date") or c.select_one(".dates"))
            date_text = date.get_text(strip=True) if date else "N/A"
            loc = (c.select_one(".location") or c.select_one(".place"))
            loc_text = loc.get_text(strip=True) if loc else "N/A"
            link_tag = c.select_one("a")
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else "N/A"

            events.append({
                "event_name": title_text,
                "start_date": date_text,
                "end_date": date_text,
                "location": loc_text,
                "sanction": "Softball Connected",
                "link": link
            })
        except Exception:
            continue

    print(f"SoftballConnected: found {len(events)} events")
    return events


# -------------------------------
# MAIN SCRAPE CONTROLLER
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
                    safe = {
                        "event_name": (e.get("event_name") or e.get("name") or e.get("Name") or "N/A"),
                        "start_date": e.get("start_date") or e.get("startDate") or "N/A",
                        "end_date": e.get("end_date") or e.get("endDate") or "N/A",
                        "location": e.get("location") or e.get("Location") or "N/A",
                        "sanction": e.get("sanction") or "N/A",
                        "link": e.get("link") or e.get("url") or "N/A",
                        "age_divisions": e.get("age_divisions") or e.get("age") or []
                    }
                    all_events.append(safe)
        except Exception as ex:
            print(f"{scraper.__name__} FAILED: {ex}")
        # small anti-block delay between scrapers
        time.sleep(random.uniform(0.5, 1.5))

    # Save JSON
    try:
        with open("fastpitch_master.json", "w", encoding="utf-8") as f:
            json.dump(all_events, f, indent=2)
    except Exception as e:
        print("Error writing JSON:", e)

    # Save CSV
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
# For server API
# -------------------------------
def get_events():
    try:
        with open("fastpitch_master.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


# quick local test when run directly
if __name__ == "__main__":
    ev = run_all_scrapers()
    print("Done. Events:", len(ev))
