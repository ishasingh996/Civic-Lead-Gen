"""
Civic Lead Generator — simple web server for Render.
Serves dashboard.html + rfp_results.json, and refreshes data every 24 hours.
"""

import subprocess
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os

REFRESH_HOURS = 24
PORT = int(os.environ.get("PORT", 10000))


def fetch_loop():
    while True:
        print("🔄  Fetching leads...", flush=True)
        try:
            subprocess.run(["python", "fetch_rfps_rss.py", "--full"], check=True)
            print("✅  Fetch complete.", flush=True)
        except Exception as e:
            print(f"⚠  Fetch failed: {e}", flush=True)
        time.sleep(REFRESH_HOURS * 3600)


# Run first fetch immediately in background
t = threading.Thread(target=fetch_loop, daemon=True)
t.start()

print(f"🌐  Serving on port {PORT}  (dashboard at /dashboard.html)", flush=True)
HTTPServer(("0.0.0.0", PORT), SimpleHTTPRequestHandler).serve_forever()
