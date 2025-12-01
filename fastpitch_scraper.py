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
    url = FREE_PROXY + "https://usssa.com/fastpitch/eventSearch/"
    events = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select(".event-row")  # USSSA uses div rows

        for row in rows:
            name = row.select_one(".event-name")
            link = row.select_one("a")
            date = row.select_one(".event-date")
            loc = row.select_one(".event-location")

            events.append(
                normalize_event({
                    "event_name": name.text.strip() if name else "",
                    "start_date": date.text.strip() if date else "",
                    "end_date": date.text.strip() if date else "",
                    "location": loc.text.strip() if loc else "",
                    "sanction": "USSSA",
                    "link": "https://usssa.com" + link["href"] if link else "",
                })
            )

    except Exception as e:
        print("USSSA error:", e)

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
    all_events = []

    scrapers = [
        scrape_usssa,
        scrape_usfa,
        scrape_pgf,
        scrape_bullpen,
        scrape_softball_connected,
    ]

    for scraper in scrapers:
        print(f"Running {scraper.__name__}...")
        data = scraper()
        all_events.extend(data)
        time.sleep(random.uniform(1, 2))  # anti-blocking delay

    # Save JSON
    with open("fastpitch_master.json", "w") as f:
        json.dump(all_events, f, indent=2)

    # Save CSV
    with open("fastpitch_master.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_name", "start_date", "end_date", "location", "sanction", "link"])
        for e in all_events:
            writer.writerow([e["event_name"], e["start_date"], e["end_date"], e["location"], e["sanction"], e["link"]])

    print(f"Scraped {len(all_events)} events.")
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
