import json
import csv
import time
import random
import requests
from bs4 import BeautifulSoup

# -------------------------------
# Global anti-blocking headers
# -------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# Optional free proxy
FREE_PROXY = "https://api.allorigins.win/raw?url="


# -------------------------------
# Normalization helper
# -------------------------------
def normalize_event(event):
    return {
        "event_name": event.get("event_name", ""),
        "start_date": event.get("start_date", ""),
        "end_date": event.get("end_date", ""),
        "location": event.get("location", ""),
        "sanction": event.get("sanction", ""),
        "link": event.get("link", ""),
        "age_divisions": event.get("age_divisions", []),
    }


# -------------------------------
# USSSA Scraper
# -------------------------------
def scrape_usssa():
    url = "https://www.usssa.com/fastpitch/eventSearch/"
    proxy_list = [
        "https://corsproxy.io/?",
        "https://thingproxy.freeboard.io/fetch/",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.usssa.com/",
    }

    events = []

    # Try direct request first
    try_urls = [{"prefix": None, "url": url}] + [
        {"prefix": p, "url": p + url} for p in proxy_list
    ]

    for attempt in try_urls:
        try:
            full_url = attempt["url"]
            print(f"USSSA: Trying {full_url}")
            r = requests.get(full_url, headers=headers, timeout=15)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "lxml")
            rows = soup.select("table tbody tr")

            # If absolute failure (blocked or no table), try next approach
            if not rows:
                print("USSSA: No result rows found, trying next method...")
                continue

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue

                event_name = cols[2].get_text(strip=True)
                event_date = cols[0].get_text(strip=True)
                event_loc = cols[4].get_text(strip=True)

                events.append({
                    "source": "USSSA",
                    "name": event_name,
                    "date": event_date,
                    "location": event_loc,
                    "url": url,
                })

            if events:
                print("USSSA: Successfully retrieved events!")
                break

        except Exception as e:
            print(f"USSSA error using {attempt['prefix']}: {e}")
            continue

    return events



# -------------------------------
# USFA Scraper
# -------------------------------
def scrape_usfa():
    url = FREE_PROXY + "https://usfastpitch.com/tournaments"
    events = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        cards = soup.select(".tournament-card")
        for c in cards:
            title = c.select_one(".title")
            date = c.select_one(".date")
            loc = c.select_one(".location")
            link = c.select_one("a")

            events.append(
                normalize_event({
                    "event_name": title.text.strip() if title else "",
                    "start_date": date.text.strip() if date else "",
                    "end_date": date.text.strip() if date else "",
                    "location": loc.text.strip() if loc else "",
                    "sanction": "USFA",
                    "link": link["href"] if link else "",
                })
            )

    except Exception as e:
        print("USFA error:", e)

    return events


# -------------------------------
# PGF Scraper
# -------------------------------
def scrape_pgf():
    url = FREE_PROXY + "https://pgfusa.com/tournaments"
    events = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        cards = soup.select(".tourney-box")
        for c in cards:
            title = c.select_one(".t-title")
            date = c.select_one(".t-date")
            loc = c.select_one(".t-loc")
            link = c.select_one("a")

            events.append(
                normalize_event({
                    "event_name": title.text.strip() if title else "",
                    "start_date": date.text.strip() if date else "",
                    "end_date": date.text.strip() if date else "",
                    "location": loc.text.strip() if loc else "",
                    "sanction": "PGF",
                    "link": link["href"] if link else "",
                })
            )

    except Exception as e:
        print("PGF error:", e)

    return events


# -------------------------------
# Bullpen Tournaments Scraper
# -------------------------------
def scrape_bullpen():
    url = FREE_PROXY + "https://play.bullpentournaments.com/events"
    events = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        rows = soup.select(".event-row")
        for row in rows:
            title = row.select_one(".name")
            date = row.select_one(".dates")
            loc = row.select_one(".location")
            link = row.select_one("a")

            events.append(
                normalize_event({
                    "event_name": title.text.strip() if title else "",
                    "start_date": date.text.strip() if date else "",
                    "end_date": date.text.strip() if date else "",
                    "location": loc.text.strip() if loc else "",
                    "sanction": "Bullpen",
                    "link": link["href"] if link else "",
                })
            )

    except Exception as e:
        print("Bullpen error:", e)

    return events


# -------------------------------
# Softball Connected Scraper
# -------------------------------
def scrape_softball_connected():
    url = FREE_PROXY + "https://softballconnected.com/tournaments"
    events = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        cards = soup.select(".tournament-card")
        for c in cards:
            title = c.select_one(".title")
            date = c.select_one(".date")
            loc = c.select_one(".location")
            link = c.select_one("a")

            events.append(
                normalize_event({
                    "event_name": title.text.strip() if title else "",
                    "start_date": date.text.strip() if date else "",
                    "end_date": date.text.strip() if date else "",
                    "location": loc.text.strip() if loc else "",
                    "sanction": "Softball Connected",
                    "link": link["href"] if link else "",
                })
            )

    except Exception as e:
        print("Softball Connected error:", e)

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
        scrape_softball_connected
    ]

    all_events = []
    for scraper in scrapers:
        try:
            events = scraper()
            if events:
                for e in events:
                    # sanitize missing keys so deployment NEVER fails
                    safe = {
                        "event_name": e.get("event_name") or e.get("name") or "N/A",
                        "start_date": e.get("start_date", "N/A"),
                        "end_date": e.get("end_date", "N/A"),
                        "location": e.get("location", "N/A"),
                        "sanction": e.get("sanction", "N/A"),
                        "link": e.get("link", "N/A")
                    }
                    all_events.append(safe)
        except Exception as ex:
            print(f"{scraper.__name__} FAILED: {ex}")

    # Write CSV safely
    with open("fastpitch_master.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_name", "start_date", "end_date", "location", "sanction", "link"])
        for e in all_events:
            writer.writerow([
                e["event_name"],
                e["start_date"],
                e["end_date"],
                e["location"],
                e["sanction"],
                e["link"]
            ])

    # Write JSON safely
    with open("fastpitch_master.json", "w", encoding="utf-8") as f:
        json.dump(all_events, f, indent=2)

    return all_events


# -------------------------------
# For server API
# -------------------------------
def get_events():
    try:
        with open("fastpitch_master.json", "r") as f:
            return json.load(f)
    except:
        return []
