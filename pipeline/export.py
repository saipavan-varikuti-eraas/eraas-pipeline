"""STAGE 5b - EXPORT: write pipeline output to BigQuery for Looker Studio.

Three export functions, one module:
  export_scored_patients  — patient risk scores (TRUNCATE per run)
  export_daily_forecast   — 7-day weather forecast rows (APPEND, deduped)
  export_location_snapshot — current hazard snapshot per location (APPEND)

Patient_id is hashed before BigQuery (PHI never reaches analytics).
"""
import os
import hashlib
import hmac
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


def _get_config():
    project = os.getenv("ERAAS_PROJECT_ID")
    env = os.getenv("ERAAS_DATASET_ENV", "dev")
    if not project:
        raise RuntimeError("ERAAS_PROJECT_ID not set in .env")
    return project, env


def _hash_id(value, salt="eraas-dev-salt"):
    if value is None:
        return None
    return hmac.new(salt.encode(), str(value).encode(), hashlib.sha256).hexdigest()


def _safe_float(v):
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(v):
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _ensure_dataset(client, project, dataset_id):
    ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ref.location = os.getenv("ERAAS_REGION", "us-east1")
    client.create_dataset(ref, exists_ok=True)


# ---------------------------------------------------------------------------
# 1. Scored patients (TRUNCATE — always the latest snapshot)
# ---------------------------------------------------------------------------
_PATIENT_SCHEMA = [
    bigquery.SchemaField("patient_id_hash", "STRING"),
    bigquery.SchemaField("tenant_id", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("disease_names", "STRING"),
    bigquery.SchemaField("comorbidities", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("zip_code", "STRING"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("lng", "FLOAT"),
    bigquery.SchemaField("adherence_score", "FLOAT"),
    bigquery.SchemaField("risk_score", "INTEGER"),
    bigquery.SchemaField("risk_category", "STRING"),
    bigquery.SchemaField("driving_hazard", "STRING"),
    bigquery.SchemaField("risk_notes", "STRING"),
    bigquery.SchemaField("permission_to_call", "BOOLEAN"),
    bigquery.SchemaField("geo_mismatch", "BOOLEAN"),
    bigquery.SchemaField("calculated_date", "DATE"),
    bigquery.SchemaField("record_inserted_at", "TIMESTAMP"),
]


def export_scored_patients(scored_patients, manifest):
    project, env = _get_config()
    tenant_id = manifest["tenant_id"]
    salt = os.getenv("ERAAS_PHI_SALT", "eraas-dev-salt")
    now = datetime.now(timezone.utc).isoformat()

    rows = [{
        "patient_id_hash": _hash_id(p.get("patient_id"), salt),
        "tenant_id": tenant_id,
        "age": _safe_int(p.get("age")),
        "gender": p.get("gender"),
        "disease_names": str(p.get("disease_names", "")),
        "comorbidities": str(p.get("comorbidities") or ""),
        "city": p.get("city"),
        "state": p.get("state"),
        "zip_code": str(p.get("zip_code", "")),
        "lat": _safe_float(p.get("lat")),
        "lng": _safe_float(p.get("long")),
        "adherence_score": _safe_float(p.get("adherence_score")),
        "risk_score": _safe_int(p.get("risk_score")),
        "risk_category": p.get("risk_category"),
        "driving_hazard": p.get("driving_hazard"),
        "risk_notes": (p.get("risk_notes") or "")[:1000],
        "permission_to_call": bool(p.get("permission_to_call")),
        "geo_mismatch": bool(p.get("geo_mismatch")),
        "calculated_date": p.get("calculated_date"),
        "record_inserted_at": now,
    } for p in scored_patients]

    client = bigquery.Client(project=project)
    dataset_id = f"patient_risk_{env}"
    table_id = f"{project}.{dataset_id}.scored_patients"
    _ensure_dataset(client, project, dataset_id)

    job_config = bigquery.LoadJobConfig(
        schema=_PATIENT_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"  BigQuery: {len(rows)} rows -> {table_id}")


# ---------------------------------------------------------------------------
# 2. Daily forecast (APPEND, deduped by run_date + tenant)
# ---------------------------------------------------------------------------
_FORECAST_SCHEMA = [
    bigquery.SchemaField("cell_lat", "FLOAT"),
    bigquery.SchemaField("cell_lng", "FLOAT"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("forecast_date", "DATE"),
    bigquery.SchemaField("forecast_run_date", "DATE"),
    bigquery.SchemaField("tenant_id", "STRING"),
    bigquery.SchemaField("temp_max", "FLOAT"),
    bigquery.SchemaField("temp_min", "FLOAT"),
    bigquery.SchemaField("temp", "FLOAT"),
    bigquery.SchemaField("feelslike_max", "FLOAT"),
    bigquery.SchemaField("feelslike_min", "FLOAT"),
    bigquery.SchemaField("feelslike", "FLOAT"),
    bigquery.SchemaField("humidity", "FLOAT"),
    bigquery.SchemaField("uv_index", "FLOAT"),
    bigquery.SchemaField("wind_speed", "FLOAT"),
    bigquery.SchemaField("precip_prob", "FLOAT"),
    bigquery.SchemaField("conditions", "STRING"),
    bigquery.SchemaField("heat_risk", "STRING"),
    bigquery.SchemaField("cold_risk", "STRING"),
    bigquery.SchemaField("extreme_heat_threshold", "FLOAT"),
    bigquery.SchemaField("extreme_cold_threshold", "FLOAT"),
    bigquery.SchemaField("record_inserted_at", "TIMESTAMP"),
]


def _classify_heat(f):
    if f is None: return None
    if f >= 125: return "Extreme Danger"
    if f >= 103: return "Danger"
    if f >= 90: return "Extreme Caution"
    if f >= 80: return "Caution"
    return "No Risk"


def _classify_cold(f):
    if f is None: return None
    if f <= -15: return "Extreme Cold"
    if f <= 15: return "Very Cold"
    if f <= 31: return "Cold"
    if f <= 49: return "Cool"
    return "No Risk"


def export_daily_forecast(grid, env_by_cell, manifest):
    project, env = _get_config()
    tenant_id = manifest["tenant_id"]
    heat_thr = manifest["enrich"].get("heat_threshold_f", 100)
    cold_thr = manifest["enrich"].get("cold_threshold_f", 32)
    now = datetime.now(timezone.utc)
    run_date = now.strftime("%Y-%m-%d")

    rows = []
    for key, cell in grid.items():
        if isinstance(key, tuple):
            lat, lng = key
            str_key = f"{lat},{lng}"
        else:
            lat, lng = map(float, key.split(","))
            str_key = key

        env_data = env_by_cell.get(str_key, {})
        city = env_data.get("aq_observed_city", "")
        state = env_data.get("aq_observed_state", "")

        vc_resp = cell.get("weather", {})
        for d in (vc_resp.get("days") or [])[:7]:
            dt_str = d.get("datetime")
            if not dt_str:
                continue
            fl_max = _safe_float(d.get("feelslikemax"))
            fl_min = _safe_float(d.get("feelslikemin"))
            rows.append({
                "cell_lat": lat, "cell_lng": lng, "city": city, "state": state,
                "forecast_date": dt_str, "forecast_run_date": run_date,
                "tenant_id": tenant_id,
                "temp_max": _safe_float(d.get("tempmax")),
                "temp_min": _safe_float(d.get("tempmin")),
                "temp": _safe_float(d.get("temp")),
                "feelslike_max": fl_max, "feelslike_min": fl_min,
                "feelslike": _safe_float(d.get("feelslike")),
                "humidity": _safe_float(d.get("humidity")),
                "uv_index": _safe_float(d.get("uvindex")),
                "wind_speed": _safe_float(d.get("windspeed")),
                "precip_prob": _safe_float(d.get("precipprob")),
                "conditions": d.get("conditions"),
                "heat_risk": _classify_heat(fl_max),
                "cold_risk": _classify_cold(fl_min),
                "extreme_heat_threshold": heat_thr,
                "extreme_cold_threshold": cold_thr,
                "record_inserted_at": now.isoformat(),
            })

    if not rows:
        print("  BigQuery: no forecast data to export")
        return

    client = bigquery.Client(project=project)
    dataset_id = f"weather_snapshot_{env}"
    table_id = f"{project}.{dataset_id}.daily_forecast"
    _ensure_dataset(client, project, dataset_id)

    # Delete today's rows first (idempotent re-run)
    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE forecast_run_date = '{run_date}' "
            f"AND tenant_id = '{tenant_id}'"
        ).result()
    except Exception:
        pass

    job_config = bigquery.LoadJobConfig(
        schema=_FORECAST_SCHEMA, write_disposition="WRITE_APPEND")
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"  BigQuery: {len(rows)} forecast rows -> {table_id}")


# ---------------------------------------------------------------------------
# 3. Location hazard snapshot (APPEND, deduped by date + tenant)
# ---------------------------------------------------------------------------
_SNAPSHOT_SCHEMA = [
    bigquery.SchemaField("cell_lat", "FLOAT"),
    bigquery.SchemaField("cell_lng", "FLOAT"),
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("tenant_id", "STRING"),
    bigquery.SchemaField("aqi", "FLOAT"),
    bigquery.SchemaField("aqi_category", "STRING"),
    bigquery.SchemaField("pm25", "FLOAT"),
    bigquery.SchemaField("pm10", "FLOAT"),
    bigquery.SchemaField("temperature", "FLOAT"),
    bigquery.SchemaField("apparent_temperature", "FLOAT"),
    bigquery.SchemaField("feelslike_max_forecast", "FLOAT"),
    bigquery.SchemaField("heat_wave_expected", "BOOLEAN"),
    bigquery.SchemaField("max_fwi_in_radius", "FLOAT"),
    bigquery.SchemaField("pollen_risk_max", "STRING"),
    bigquery.SchemaField("ili_risk_today", "STRING"),
    bigquery.SchemaField("ili_usable", "BOOLEAN"),
    bigquery.SchemaField("record_inserted_at", "TIMESTAMP"),
]


def export_location_snapshot(env_by_cell, manifest):
    project, env = _get_config()
    tenant_id = manifest["tenant_id"]
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    rows = [{
        "cell_lat": _safe_float(e.get("cell_lat")),
        "cell_lng": _safe_float(e.get("cell_lng")),
        "snapshot_date": today, "tenant_id": tenant_id,
        "aqi": _safe_float(e.get("aqi")),
        "aqi_category": e.get("aqi_category"),
        "pm25": _safe_float(e.get("pm25")),
        "pm10": _safe_float(e.get("pm10")),
        "temperature": _safe_float(e.get("temperature")),
        "apparent_temperature": _safe_float(e.get("apparent_temperature")),
        "feelslike_max_forecast": _safe_float(e.get("feelslike_max_forecast")),
        "heat_wave_expected": e.get("heat_wave_expected"),
        "max_fwi_in_radius": _safe_float(e.get("max_fwi_in_radius")),
        "pollen_risk_max": e.get("pollen_risk_max"),
        "ili_risk_today": e.get("ili_risk_today"),
        "ili_usable": e.get("ili_usable"),
        "record_inserted_at": now.isoformat(),
    } for e in env_by_cell.values()]

    client = bigquery.Client(project=project)
    dataset_id = f"weather_snapshot_{env}"
    table_id = f"{project}.{dataset_id}.location_hazards"
    _ensure_dataset(client, project, dataset_id)

    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE snapshot_date = '{today}' "
            f"AND tenant_id = '{tenant_id}'"
        ).result()
    except Exception:
        pass

    job_config = bigquery.LoadJobConfig(
        schema=_SNAPSHOT_SCHEMA, write_disposition="WRITE_APPEND")
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"  BigQuery: {len(rows)} snapshot rows -> {table_id}")