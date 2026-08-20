"""One-location dry run of the new observed-history fetch.
Confirms VC auth, URL date-range format, and response parsing BEFORE
committing a full 45-day pull across all 10 locations.

Run from repo root:  python scratch_history_check.py
Writes nothing to BigQuery. Only hits VC for a single cell.
"""
import yaml
from datetime import datetime, timezone
from pipeline.enrich import fetch_history

manifest = yaml.safe_load(open("manifests/health_plan_b.yaml"))
cfg = manifest["enrich"]
hist_cfg = cfg["hazards"]["weather_history"]
providers = cfg["providers"]
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# one Houston cell
data, src = fetch_history(hist_cfg, 29.76, -95.37, providers,
                          "data", today, cfg["history_days"])

print("source:", src)
if "_error" in data:
    print("ERROR:", data)
else:
    days = data.get("days", [])
    print(f"got {len(days)} days (expected ~{cfg['history_days']+1})")
    print("first 3:")
    for d in days[:3]:
        print("  ", d.get("datetime"), "tempmax=", d.get("tempmax"),
              "feelslikemax=", d.get("feelslikemax"))
    print("last 2:")
    for d in days[-2:]:
        print("  ", d.get("datetime"), "tempmax=", d.get("tempmax"))