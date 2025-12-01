from flask import Flask, jsonify
from fastpitch_scraper import run_all_scrapers, get_events
import os
import threading

app = Flask(__name__)

# ----------------------------
# If JSON file doesn't exist, generate it
# ----------------------------
if not os.path.exists("fastpitch_master.json"):
    print("No data file found — running scraper once to initialize...")
    run_all_scrapers()


# ----------------------------
# Background refresh thread
# ----------------------------
def refresh_data():
    print("Running scheduled scrape...")
    run_all_scrapers()
    print("Scrape finished.")


# ----------------------------
# API endpoint
# ----------------------------
@app.route("/events", methods=["GET"])
def events():
    data = get_events()
    return jsonify({
        "count": len(data),
        "events": data
    })


# ----------------------------
# Manual scrape endpoint (optional)
# ----------------------------
@app.route("/scrape-now", methods=["POST"])
def scrape_now():
    threading.Thread(target=refresh_data).start()
    return jsonify({"status": "Scrape started"})


# ----------------------------
# Root test
# ----------------------------
@app.route("/", methods=["GET"])
def home():
    return "Fastpitch Scraper API is running."


# ----------------------------
# Run local server (Render uses gunicorn instead)
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
