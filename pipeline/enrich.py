"""STAGE 2 - ENRICH: fetch hazard data per LOCATION, not per patient.

The fetch loop is vendor-agnostic: providers handle auth and location shape,
the manifest says which provider serves which hazard. Cache key includes the
provider to prevent shape mismatches (lesson C5).

Providers are defined inline — each is just a function that builds a request
dict {url, headers, params} from a manifest config block.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Providers: how each vendor authenticates and takes a location
# ---------------------------------------------------------------------------
def _ambee_request(path, lat, lng, cfg):
    return {
        "url": f"{cfg['base_url']}{path}",
        "headers": {"x-api-key": os.getenv(cfg["api_key_env"]),
                    "Content-type": "application/json"},
        "params": {"lat": lat, "lng": lng},
    }


def _vc_request(path, lat, lng, cfg):
    location = f"{lat},{lng}"
    return {
        "url": f"{cfg['base_url']}{path.format(location=location)}",
        "headers": {},
        "params": {"key": os.getenv(cfg["api_key_env"]),
                   "unitGroup": cfg.get("unit_group", "us"),
                   "contentType": "json",
                   "include": cfg.get("include", "days,current")},
    }


def _vc_history_request(path, lat, lng, cfg, start, end):
    """VC Timeline historical range: /timeline/{location}/{start}/{end}.

    Same endpoint, same auth, same days[] response shape as the forecast
    request — only the URL carries a date range. VC blends real observations
    up to today with a nowcast for today itself; the export keys each row by
    its own date and does NOT stamp forecast_run_date, keeping observed rows
    distinct from the forward forecast table.
    """
    location = f"{lat},{lng}"
    return {
        "url": f"{cfg['base_url']}{path.format(location=location, start=start, end=end)}",
        "headers": {},
        "params": {"key": os.getenv(cfg["api_key_env"]),
                   "unitGroup": cfg.get("unit_group", "us"),
                   "contentType": "json",
                   # observed history only needs daily rows, not current/hourly
                   "include": cfg.get("history_include", "days")},
    }


_PROVIDERS = {"ambee": _ambee_request, "visual_crossing": _vc_request}


def _build_request(provider, path, lat, lng, providers_cfg):
    if provider not in _PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}. "
                         f"registered: {sorted(_PROVIDERS)}")
    return _PROVIDERS[provider](path, lat, lng, providers_cfg[provider])


# ---------------------------------------------------------------------------
# Core fetch logic
# ---------------------------------------------------------------------------
def distinct_locations(patients, lat_field, lng_field, precision):
    """Collapse patients → distinct grid cells. This is the cost saving."""
    cells = {}
    for p in patients:
        key = (round(float(p[lat_field]), precision),
               round(float(p[lng_field]), precision))
        cells.setdefault(key, []).append(p["patient_id"])
    return cells


def _cache_path(root, day, hazard, provider, lat, lng):
    return os.path.join(root, "weather_cache", day, hazard, provider,
                        f"{lat}_{lng}.json")


def fetch_hazard(hazard, hcfg, lat, lng, providers_cfg, cache_root, day,
                 timeout=30):
    """One location, one hazard, whichever vendor serves it."""
    provider = hcfg["provider"]
    cp = _cache_path(cache_root, day, hazard, provider, lat, lng)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f), "cache"

    req = _build_request(provider, hcfg["path"], lat, lng, providers_cfg)

    for attempt in range(3):
        try:
            r = requests.get(req["url"], headers=req["headers"],
                             params=req["params"], timeout=timeout)
        except requests.RequestException as e:
            if attempt == 2:
                return {"_error": "network", "_detail": str(e)[:200]}, "error"
            time.sleep(2 ** attempt)
            continue

        if r.status_code in (200, 206):
            if r.status_code == 206:
                print(f"    WARNING 206 {hazard}/{provider} {lat},{lng}: "
                      f"QUOTA EXHAUSTED - data is TRIMMED")
            try:
                data = r.json()
            except ValueError:
                return {"_error": "bad_json", "_body": r.text[:200]}, "error"
            os.makedirs(os.path.dirname(cp), exist_ok=True)
            with open(cp, "w") as f:
                json.dump(data, f, indent=2)
            return data, ("api" if r.status_code == 200 else "partial")

        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(
                f"{r.status_code} auth failed for {provider!r} — "
                f"check {providers_cfg[provider]['api_key_env']} in .env")
        if r.status_code == 422:
            raise RuntimeError(f"422 quota exceeded for {provider!r}. Stop.")
        return {"_error": r.status_code, "_body": r.text[:200]}, "error"

    return {"_error": 429, "_detail": "rate limited after 3 attempts"}, "error"


def fetch_history(hcfg, lat, lng, providers_cfg, cache_root, day,
                  history_days, timeout=30):
    """Fetch the past `history_days` of OBSERVED weather for one location.

    Runs once per location per run. Cached under weather_history/{day}/ so a
    given run's backfill is fetched only once. Returns the same days[] shape
    the forecast path returns, so the export can iterate it identically.
    """
    from datetime import timedelta
    provider = hcfg["provider"]
    if provider != "visual_crossing":
        raise ValueError("weather_history is only supported on visual_crossing")

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=history_days)
    start_s, end_s = start.isoformat(), end.isoformat()

    cp = _cache_path(cache_root, day, "weather_history", provider, lat, lng)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f), "cache"

    req = _vc_history_request(hcfg["path"], lat, lng,
                              providers_cfg[provider], start_s, end_s)

    for attempt in range(3):
        try:
            r = requests.get(req["url"], headers=req["headers"],
                             params=req["params"], timeout=timeout)
        except requests.RequestException as e:
            if attempt == 2:
                return {"_error": "network", "_detail": str(e)[:200]}, "error"
            time.sleep(2 ** attempt)
            continue

        if r.status_code in (200, 206):
            if r.status_code == 206:
                print(f"    WARNING 206 weather_history/{provider} "
                      f"{lat},{lng}: QUOTA EXHAUSTED - data is TRIMMED")
            try:
                data = r.json()
            except ValueError:
                return {"_error": "bad_json", "_body": r.text[:200]}, "error"
            os.makedirs(os.path.dirname(cp), exist_ok=True)
            with open(cp, "w") as f:
                json.dump(data, f, indent=2)
            return data, ("api" if r.status_code == 200 else "partial")

        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        if r.status_code in (401, 403):
            raise RuntimeError(
                f"{r.status_code} auth failed for {provider!r} — "
                f"check {providers_cfg[provider]['api_key_env']} in .env")
        if r.status_code == 422:
            raise RuntimeError(f"422 quota exceeded for {provider!r}. Stop.")
        return {"_error": r.status_code, "_body": r.text[:200]}, "error"

    return {"_error": 429, "_detail": "rate limited after 3 attempts"}, "error"


def enrich(manifest, patients, out_root="data"):
    cfg = manifest["enrich"]
    providers_cfg = cfg["providers"]
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    cells = distinct_locations(patients,
                               manifest["source"]["lat_field"],
                               manifest["source"]["lng_field"],
                               cfg["grid_precision"])

    # weather_history is fetched via its own range-aware path, not the
    # per-hazard "latest" loop — pull it out so the loop stays uniform.
    hist_cfg = cfg["hazards"].get("weather_history")
    hazards = {h: hc for h, hc in cfg["hazards"].items()
               if h != "weather_history"}
    history_days = cfg.get("history_days", 45)

    by_provider = {}
    for h, hc in hazards.items():
        by_provider.setdefault(hc["provider"], []).append(h)
    print(f"{len(patients)} patients -> {len(cells)} locations")
    for prov, hz in sorted(by_provider.items()):
        print(f"  {prov:16s} {len(hz)} hazards x {len(cells)} = "
              f"{len(hz)*len(cells):3d} calls  ({', '.join(sorted(hz))})")
    print()

    grid = {}
    stats = {"api": 0, "cache": 0, "partial": 0, "error": 0}
    for (lat, lng) in sorted(cells):
        grid[(lat, lng)] = {}
        for hazard, hcfg in hazards.items():
            data, src = fetch_hazard(hazard, hcfg, lat, lng, providers_cfg,
                                     out_root, day)
            grid[(lat, lng)][hazard] = data
            stats[src] += 1
            if src == "error":
                print(f"    ERROR {hazard}/{hcfg['provider']} {lat},{lng}: "
                      f"{data.get('_error')}")
        # observed history: once per location, past `history_days` days
        if hist_cfg:
            hdata, hsrc = fetch_history(hist_cfg, lat, lng, providers_cfg,
                                        out_root, day, history_days)
            grid[(lat, lng)]["weather_history"] = hdata
            stats[hsrc] += 1
            if hsrc == "error":
                print(f"    ERROR weather_history {lat},{lng}: "
                      f"{hdata.get('_error')}")
        print(f"  {lat},{lng}  ({len(cells[(lat, lng)])} patients) done")

    print(f"\nstats: {stats}")

    ref = os.path.join(out_root, "weather_reference", day)
    os.makedirs(ref, exist_ok=True)
    with open(os.path.join(ref, "grid.json"), "w") as f:
        json.dump({f"{k[0]},{k[1]}": v for k, v in grid.items()}, f, indent=2)
    print(f"wrote {ref}/grid.json")
    return grid, cells