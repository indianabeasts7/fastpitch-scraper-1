import os
import json
import csv
import time
import requests
from bs4 import BeautifulSoup

# ============================================
# ENV VARS + GLOBAL SETTINGS
# ============================================

SCRAPINGANT_KEY = os.getenv("SCRAPINGANT_KEY")
SCRAPINGANT_URL = "https://api.scrapingant.com/v2/general"
SCRAPINGANT_TIMEOUT = 45
SCRAPINGANT_RETRIES = 3

DATA_FILE = "fastpitch_master.json"
CSV_FILE = "fastpitch_master.csv"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9"
}

# ============================================
# SCRAPINGANT FETCHER
# ============================================
def fetch_via_scrapingant(target_url):
    """
    Works with your account's ScrapingAnt General API (GET, key in query string).
    Returns HTML string or None.
    """
    if not SCRAPINGANT_KEY:
        print("ERROR: Missing SCRAPINGANT_KEY")
        return None

    import urllib.parse
    encoded_url = urllib.parse.quote(target_url, safe="")

    api_url = (
        f"https://api.scrapingant.com/v2/general"
        f"?url={encoded_url}"
        f"&x-api-key={SCRAPINGANT_KEY}"
        f"&browser=chrome"
        f"&proxy_type=datacenter"
        f"&render=true"
    )

    delay = 1
    for attempt in range(1, SCRAPINGANT_RETRIES + 1):
        try:
            print(f"ScrapingAnt GET attempt {attempt} → {target_url}")
            r = requests.get(api_url, timeout=SCRAPINGANT_TIMEOUT)
            r.raise_for_status()

            data = r.json()
            html = data.get("content")

            if html:
                print("ScrapingAnt success")
                return html
            else:
                print("ScrapingAnt returned no content")

        except Exception as e:
            print(f"ScrapingAnt attempt {attempt} failed: {e}")

            if attempt < SCRAPINGANT_RETRIES:
                time.sleep(delay)
                delay *= 2

    print("ScrapingAnt: all retries failed")
    return None



# ============================================
# UTILITY: STANDARDIZED EVENT FORMAT
# ============================================

def make_event(name, start, end, location, sanction, link):
    return {
        "event_name": name.strip() if name else "N/A",
        "start_date": start.strip() if start else "N/A",
        "end_date": end.strip() if end else "N/A",
        "location": location.strip() if location else "N/A",
        "sanction": sanction.strip() if sanction else "N/A",
        "link": link if link else "N/A"
    }

# ============================================
# USSSA SCRAPER (via JS-rendered page)
# ============================================

def scrape_usssa():
    print("Running scrape_usssa...")

    url = "https://usssa.com/fastpitch/eventSearch/"
    html = fetch_via_scrapingant(url)

    if not html:
        print("USSSA: no HTML returned")
        return []

    soup = BeautifulSoup(html, "html.parser")

    # USSSA embeds events in JS variable "var preloadedEvents = [...]"
    events = []

    try:
        script = next(
            (s.text for s in soup.find_all("script") if "preloadedEvents" in s.text),
            None
        )
        if not script:
            print("USSSA: preloadedEvents not found")
            return []

        json_start = script.find("[")
        json_end = script.rfind("]") + 1
        raw_json = script[json_start:json_end]

        data = json.loads(raw_json)

        for e in data:
            events.append(
                make_event(
                    e.get("event_name"),
                    e.get("start_date"),
                    e.get("end_date"),
                    e.get("city"),
                    "USSSA",
                    "https://usssa.com/fastpitch/eventSearch"
                )
            )
        print(f"USSSA: extracted {len(events)} events")
        return events

    except Exception as e:
        print(f"USSSA parsing error: {e}")
        return []

# ============================================
# USFA SCRAPER
# ============================================

def scrape_usfa():
    print("Running scrape_usfa...")

    url = "https://usfastpitch.com/tournaments"
    html = fetch_via_scrapingant(url)

    if not html:
        print("USFA: no HTML returned")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    for card in soup.select(".tournament-card"):
        name = card.select_one(".tournament-title")
        loc = card.select_one(".tournament-location")
        dates = card.select_one(".tournament-dates")

        events.append(
            make_event(
                name.text if name else None,
                dates.text if dates else None,
                None,
                loc.text if loc else None,
                "USFA",
                url
            )
        )

    print(f"USFA: extracted {len(events)} events")
    return events

# ============================================
# PGF SCRAPER
# ============================================

def scrape_pgf():
    print("Running scrape_pgf...")

    url = "https://pgfusa.com/tournaments"
    html = fetch_via_scrapingant(url)

    if not html:
        print("PGF: no HTML returned")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    rows = soup.select("table tbody tr")
    for row in rows:
        cols = [c.text.strip() for c in row.find_all("td")]
        if len(cols) >= 3:
            events.append(
                make_event(
                    cols[0],
                    cols[1],
                    None,
                    cols[2],
                    "PGF",
                    url
                )
            )

    print(f"PGF: extracted {len(events)} events")
    return events

# ============================================
# BULLPEN TOURNAMENTS
# ============================================

def scrape_bullpen():
    print("Running scrape_bullpen...")

    url = "https://play.bullpentournaments.com/events"
    html = fetch_via_scrapingant(url)

    if not html:
        print("Bullpen: no HTML")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    for card in soup.select(".event-card"):
        title = card.select_one(".event-name")
        date = card.select_one(".event-date")
        city = card.select_one(".event-location")

        events.append(
            make_event(
                title.text if title else None,
                date.text if date else None,
                None,
                city.text if city else None,
                "Bullpen",
                url
            )
        )

    print(f"Bullpen: extracted {len(events)} events")
    return events

# ============================================
# SOFTBALL CONNECTED
# ============================================

def scrape_softball_connected():
    print("Running scrape_softball_connected...")

    url = "https://softballconnected.com/tournaments"
    html = fetch_via_scrapingant(url)

    if not html:
        print("SoftballConnected: no HTML")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    for row in soup.select(".tournament-row"):
        title = row.select_one(".tournament-title")
        dates = row.select_one(".tournament-dates")
        city = row.select_one(".tournament-location")

        events.append(
            make_event(
                title.text if title else None,
                dates.text if dates else None,
                None,
                city.text if city else None,
                "SoftballConnected",
                url
            )
        )

    print(f"SoftballConnected: extracted {len(events)} events")
    return events

# ============================================
# RUN ALL SCRAPERS
# ============================================

def run_all_scrapers():
    print("Running ALL scrapers...")

    all_events = []
    all_events.extend(scrape_usssa())
    all_events.extend(scrape_usfa())
    all_events.extend(scrape_pgf())
    all_events.extend(scrape_bullpen())
    all_events.extend(scrape_softball_connected())

    print(f"TOTAL events scraped: {len(all_events)}")

    # Save JSON
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump({"count": len(all_events), "events": all_events}, f, indent=2)

    # Save CSV
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_name", "start_date", "end_date", "location", "sanction", "link"])
        for e in all_events:
            writer.writerow([
                e["event_name"], e["start_date"], e["end_date"],
                e["location"], e["sanction"], e["link"]
            ])

    return all_events

# ============================================
# MAIN (for local batch runs)
# ============================================

if __name__ == "__main__":
    run_all_scrapers()


