"""Enrich from MongoDB: read hazard data from the team's 'eraas' database.

The team's pipeline (Apache Beam / Dataflow) already fetches weather data
from Ambee/VC, processes it, and writes it to:
  - BigQuery (6 hazard datasets)
  - MongoDB 'eraas' db (4 collections: wildfire_data, airquality_data,
    extremecold_data, pollen_data)

Instead of calling APIs again for the SAME data, our pipeline reads from
their MongoDB output. This ensures:
  1. Both pipelines use the SAME environmental data (consistency)
  2. No duplicate API calls (cost)
  3. The team's data validation and transforms are reused

FALLBACK: if the team's data doesn't cover a patient's location, we fall
back to direct API calls (the existing enrich.py). The manifest controls
which mode to use.

Collection schemas (from the team's DomainSpec + bq_to_mongo.py):
  airquality_data:  lat, lng, timestamp, aqi, category, pm2_5, pm10, no2, ozone, so2, co
  wildfire_data:    lat, lng, fwi_today, ffmc_today, days_since_last_fire, predicted_risk
  extremecold_data: datetime, zipcode, feelslike, feelslikemin, snow, snowdepth
  pollen_data:      ts, actual_grass_count, actual_tree_count, actual_weed_count,
                    forecast_grass_count, forecast_tree_count, forecast_weed_count
"""
from .mongo_client import get_database

ERAAS_DB = "eraas"

# Map our hazard names to team's collection names and key fields
COLLECTION_MAP = {
    "air_quality": {
        "collection": "airquality_data",
        "geo_fields": ("lat", "lng"),
        "extract": lambda doc: {
            "aqi": doc.get("aqi"),
            "aqi_category": doc.get("category"),
            "pm25": doc.get("pm2_5"),
            "pm10": doc.get("pm10"),
            "no2": doc.get("no2"),
            "ozone": doc.get("ozone"),
            "so2": doc.get("so2"),
            "co": doc.get("co"),
            "aq_observed_zip": str(doc.get("zip_code", "")),
            "aq_updated_at": str(doc.get("timestamp", "")),
            "aq_station_count": 1,
        },
    },
    "wildfire": {
        "collection": "wildfire_data",
        "geo_fields": ("lat", "lng"),
        "extract": lambda doc: {
            "max_fwi_in_radius": doc.get("fwi_today"),
            "nearest_fire_fwi": doc.get("fwi_today"),
            "days_since_last_fire": doc.get("days_since_last_fire"),
            "fire_count_in_radius": 1,
            "fire_count_reliable": False,
            "wildfire_radius_km": None,  # pre-aggregated by team's pipeline
        },
    },
    "weather": {
        "collection": "extremecold_data",
        "geo_fields": None,  # this collection is zip-keyed, not lat/lng
        "zip_field": "zipcode",
        "extract": lambda doc: {
            "temperature": doc.get("temp"),
            "apparent_temperature": doc.get("feelslike"),
            "feelslike_min_forecast": doc.get("feelslikemin"),
            "humidity": None,
            "wind_speed": None,
            "weather_summary": None,
        },
    },
    "pollen": {
        "collection": "pollen_data",
        "geo_fields": None,  # nested in 'source' field
        "extract": lambda doc: {
            "grass_pollen_count": doc.get("actual_grass_count") or doc.get("forecast_grass_count"),
            "tree_pollen_count": doc.get("actual_tree_count") or doc.get("forecast_tree_count"),
            "weed_pollen_count": doc.get("actual_weed_count") or doc.get("forecast_weed_count"),
            "grass_pollen_risk": None,  # team's pipeline doesn't carry risk labels
            "tree_pollen_risk": None,
            "weed_pollen_risk": None,
        },
    },
}


def _find_nearest(collection, lat, lng, max_distance_km=50):
    """Find the nearest document to a lat/lng within a radius.

    Uses a simple distance filter. In production with GeoJSON indexes,
    this would use $near or $geoNear for real spatial queries.
    """
    tolerance = max_distance_km / 111.0  # rough degrees
    query = {
        "lat": {"$gte": lat - tolerance, "$lte": lat + tolerance},
        "lng": {"$gte": lng - tolerance, "$lte": lng + tolerance},
    }
    # get the most recent
    doc = collection.find_one(query, sort=[("timestamp", -1)])
    return doc


def enrich_from_mongo(patients, manifest):
    """Read hazard data from the team's eraas MongoDB.

    Returns the same structure as the API-based enrich: a dict keyed by
    (lat,lng) string with hazard fields per location.
    """
    db = get_database(ERAAS_DB)
    prec = manifest["enrich"]["grid_precision"]
    lat_f = manifest["source"]["lat_field"]
    lng_f = manifest["source"]["lng_field"]

    # dedupe patients to distinct locations (same principle as API enrich)
    cells = {}
    for p in patients:
        key = f"{round(float(p[lat_f]), prec)},{round(float(p[lng_f]), prec)}"
        cells.setdefault(key, []).append(p.get("patient_id"))

    print(f"  {len(patients)} patients -> {len(cells)} locations")
    print(f"  reading from MongoDB '{ERAAS_DB}' database")

    available = db.list_collection_names()
    print(f"  available collections: {available}")

    env_by_cell = {}
    hits, misses = 0, 0

    for key in sorted(cells):
        lat, lng = map(float, key.split(","))
        flat = {"cell_lat": lat, "cell_lng": lng}

        for hazard, cfg in COLLECTION_MAP.items():
            coll_name = cfg["collection"]
            if coll_name not in available:
                continue

            coll = db[coll_name]
            doc = None

            if cfg.get("geo_fields"):
                doc = _find_nearest(coll, lat, lng)
            elif cfg.get("zip_field"):
                # zip-keyed: need patient's zip, check later
                pass

            if doc:
                flat.update(cfg["extract"](doc))
                hits += 1
            else:
                misses += 1

        env_by_cell[key] = flat

    print(f"  lookups: {hits} hits, {misses} misses from team's data")
    return env_by_cell


def check_coverage(patients, manifest):
    """Check how much of our patient locations the team's data covers.

    Run this BEFORE deciding whether to use mongo or API enrichment.
    """
    db = get_database(ERAAS_DB)
    lat_f = manifest["source"]["lat_field"]
    lng_f = manifest["source"]["lng_field"]
    prec = manifest["enrich"]["grid_precision"]

    cells = set()
    for p in patients:
        key = f"{round(float(p[lat_f]), prec)},{round(float(p[lng_f]), prec)}"
        cells.add(key)

    print(f"\n=== COVERAGE CHECK: team's eraas db vs our {len(cells)} locations ===")
    available = db.list_collection_names()

    for hazard, cfg in COLLECTION_MAP.items():
        coll_name = cfg["collection"]
        if coll_name not in available:
            print(f"  {hazard:15s} collection '{coll_name}' NOT FOUND")
            continue

        coll = db[coll_name]
        total = coll.count_documents({})
        covered = 0

        if cfg.get("geo_fields"):
            for key in cells:
                lat, lng = map(float, key.split(","))
                if _find_nearest(coll, lat, lng):
                    covered += 1

        print(f"  {hazard:15s} {total:4d} docs in team's DB | "
              f"covers {covered}/{len(cells)} of our locations")

    print()