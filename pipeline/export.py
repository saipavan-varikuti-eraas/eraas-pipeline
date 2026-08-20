"""STAGE 5b - EXPORT: all BigQuery exports in one module.

7 exports to their correct domain datasets:
  1. scored_patients      → patient_risk_{env}            (TRUNCATE)
  2. daily_forecast       → weather_snapshot_{env}         (APPEND deduped)
  3. location_snapshot     → weather_snapshot_{env}         (APPEND deduped)
  4. extremeheat          → extremeheat_forecast_{env}     (TRUNCATE)
  5. severe_weather       → severe_weather_{env}           (TRUNCATE)
  6. wildfire_snapshot    → wildfire_forecast_{env}         (TRUNCATE)
  7. aqi_snapshot         → airquality_forecast_{env}      (TRUNCATE)
"""
import os, hashlib, hmac
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
load_dotenv()

def _cfg():
    p = os.getenv("ERAAS_PROJECT_ID")
    e = os.getenv("ERAAS_DATASET_ENV") or "dev"
    r = os.getenv("ERAAS_REGION", "us-east1")
    if not p: raise RuntimeError("ERAAS_PROJECT_ID not set")
    return p, e, r

def _hash(v, salt="eraas-dev-salt"):
    return hmac.new(salt.encode(), str(v).encode(), hashlib.sha256).hexdigest() if v else None

def _f(v):
    try: return float(v) if v is not None else None
    except: return None

def _i(v):
    try: return int(v) if v is not None else None
    except: return None

def _ds(client, project, dsid, region):
    ref = bigquery.Dataset(f"{project}.{dsid}"); ref.location = region
    client.create_dataset(ref, exists_ok=True)

def _write(client, project, dsid, tbl, schema, rows, disp, region, dedup=None):
    _ds(client, project, dsid, region)
    tid = f"{project}.{dsid}.{tbl}"
    if dedup:
        try: client.query(dedup).result()
        except: pass
    client.load_table_from_json(rows, tid, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition=disp)).result()
    return tid

def _heat(f):
    if f is None: return None
    if f>=125: return "Extreme Danger"
    if f>=103: return "Danger"
    if f>=90: return "Extreme Caution"
    if f>=80: return "Caution"
    return "No Risk"

def _cold(f):
    if f is None: return None
    if f<=-15: return "Extreme Cold"
    if f<=15: return "Very Cold"
    if f<=31: return "Cold"
    if f<=49: return "Cool"
    return "No Risk"

def _grid_iter(grid, env_by_cell):
    for key, cell in grid.items():
        if isinstance(key, tuple): lat, lng = key; sk = f"{lat},{lng}"
        else: lat, lng = map(float, key.split(",")); sk = key
        env = env_by_cell.get(sk, {})
        yield lat, lng, cell, env

# 1
def export_scored_patients(scored, manifest):
    p, e, r = _cfg(); now = datetime.now(timezone.utc).isoformat()
    salt = os.getenv("ERAAS_PHI_SALT", "eraas-dev-salt")
    rows = [{"patient_id_hash":_hash(x.get("patient_id"),salt),"tenant_id":manifest["tenant_id"],
        "age":_i(x.get("age")),"gender":x.get("gender"),"disease_names":str(x.get("disease_names","")),
        "comorbidities":str(x.get("comorbidities") or ""),"city":x.get("city"),"state":x.get("state"),
        "zip_code":str(x.get("zip_code","")),"lat":_f(x.get("lat")),"lng":_f(x.get("long")),
        "adherence_score":_f(x.get("adherence_score")),"risk_score":_i(x.get("risk_score")),
        "risk_category":x.get("risk_category"),"driving_hazard":x.get("driving_hazard"),
        "risk_notes":(x.get("risk_notes") or "")[:1000],"permission_to_call":bool(x.get("permission_to_call")),
        "geo_mismatch":bool(x.get("geo_mismatch")),"calculated_date":x.get("calculated_date"),
        "record_inserted_at":now} for x in scored]
    s = [bigquery.SchemaField(k,t) for k,t in [("patient_id_hash","STRING"),("tenant_id","STRING"),
        ("age","INTEGER"),("gender","STRING"),("disease_names","STRING"),("comorbidities","STRING"),
        ("city","STRING"),("state","STRING"),("zip_code","STRING"),("lat","FLOAT"),("lng","FLOAT"),
        ("adherence_score","FLOAT"),("risk_score","INTEGER"),("risk_category","STRING"),
        ("driving_hazard","STRING"),("risk_notes","STRING"),("permission_to_call","BOOLEAN"),
        ("geo_mismatch","BOOLEAN"),("calculated_date","DATE"),("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p)
    tid = _write(c, p, f"patient_risk_{e}", "scored_patients", s, rows, "WRITE_TRUNCATE", r)
    print(f"  BQ: {len(rows)} rows -> {tid}")

# 2
def export_daily_forecast(grid, env_by_cell, manifest):
    p, e, r = _cfg(); tid_m = manifest["tenant_id"]
    ht = manifest["enrich"].get("heat_threshold_f", 100)
    now = datetime.now(timezone.utc); rd = now.strftime("%Y-%m-%d")
    rows = []
    for lat, lng, cell, env in _grid_iter(grid, env_by_cell):
        for d in (cell.get("weather",{}).get("days") or [])[:7]:
            dt=d.get("datetime");
            if not dt: continue
            fl=_f(d.get("feelslikemax")); fm=_f(d.get("feelslikemin"))
            rows.append({"cell_lat":lat,"cell_lng":lng,"city":env.get("aq_observed_city",""),
                "state":env.get("aq_observed_state",""),"forecast_date":dt,"forecast_run_date":rd,
                "tenant_id":tid_m,"temp_max":_f(d.get("tempmax")),"temp_min":_f(d.get("tempmin")),
                "feelslike_max":fl,"feelslike_min":fm,"humidity":_f(d.get("humidity")),
                "conditions":d.get("conditions"),"heat_risk":_heat(fl),"cold_risk":_cold(fm),
                "extreme_heat_threshold":ht,"record_inserted_at":now.isoformat()})
    if not rows: return
    s = [bigquery.SchemaField(k,t) for k,t in [("cell_lat","FLOAT"),("cell_lng","FLOAT"),
        ("city","STRING"),("state","STRING"),("forecast_date","DATE"),("forecast_run_date","DATE"),
        ("tenant_id","STRING"),("temp_max","FLOAT"),("temp_min","FLOAT"),("feelslike_max","FLOAT"),
        ("feelslike_min","FLOAT"),("humidity","FLOAT"),("conditions","STRING"),("heat_risk","STRING"),
        ("cold_risk","STRING"),("extreme_heat_threshold","FLOAT"),("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p); dsid = f"weather_snapshot_{e}"
    dedup = f"DELETE FROM `{p}.{dsid}.daily_forecast` WHERE forecast_run_date='{rd}' AND tenant_id='{tid_m}'"
    tid = _write(c, p, dsid, "daily_forecast", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} forecast rows -> {tid}")

# 3
def export_location_snapshot(env_by_cell, manifest):
    p, e, r = _cfg(); tid_m = manifest["tenant_id"]
    now = datetime.now(timezone.utc); td = now.strftime("%Y-%m-%d")
    rows = [{"cell_lat":_f(v.get("cell_lat")),"cell_lng":_f(v.get("cell_lng")),
        "snapshot_date":td,"tenant_id":tid_m,"aqi":_f(v.get("aqi")),
        "aqi_category":v.get("aqi_category"),"pm25":_f(v.get("pm25")),
        "temperature":_f(v.get("temperature")),"apparent_temperature":_f(v.get("apparent_temperature")),
        "feelslike_max_forecast":_f(v.get("feelslike_max_forecast")),
        "heat_wave_expected":v.get("heat_wave_expected"),"max_fwi_in_radius":_f(v.get("max_fwi_in_radius")),
        "pollen_risk_max":v.get("pollen_risk_max"),"ili_risk_today":v.get("ili_risk_today"),
        "record_inserted_at":now.isoformat()} for v in env_by_cell.values()]
    s = [bigquery.SchemaField(k,t) for k,t in [("cell_lat","FLOAT"),("cell_lng","FLOAT"),
        ("snapshot_date","DATE"),("tenant_id","STRING"),("aqi","FLOAT"),("aqi_category","STRING"),
        ("pm25","FLOAT"),("temperature","FLOAT"),("apparent_temperature","FLOAT"),
        ("feelslike_max_forecast","FLOAT"),("heat_wave_expected","BOOLEAN"),
        ("max_fwi_in_radius","FLOAT"),("pollen_risk_max","STRING"),("ili_risk_today","STRING"),
        ("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p); dsid = f"weather_snapshot_{e}"
    dedup = f"DELETE FROM `{p}.{dsid}.location_hazards` WHERE snapshot_date='{td}' AND tenant_id='{tid_m}'"
    tid = _write(c, p, dsid, "location_hazards", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} snapshot rows -> {tid}")

# 4
def export_extremeheat(grid, env_by_cell, manifest):
    p, e, r = _cfg(); ht = manifest["enrich"].get("heat_threshold_f",100)
    now = datetime.now(timezone.utc).isoformat(); rd = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for lat, lng, cell, env in _grid_iter(grid, env_by_cell):
        for d in (cell.get("weather",{}).get("days") or [])[:7]:
            dt=d.get("datetime");
            if not dt: continue
            fl=d.get("feelslikemax") or 0
            rows.append({"date":dt,"forecast_run_date":rd,"city":env.get("aq_observed_city",""),
                "state":env.get("aq_observed_state",""),"lat":lat,"lng":lng,
                "temp_max":d.get("tempmax"),"feelslike_max":d.get("feelslikemax"),
                "feelslike_min":d.get("feelslikemin"),"humidity":d.get("humidity"),
                "heat_risk":_heat(fl),"extreme_heat_threshold":ht,"record_inserted_at":now})
    if not rows: return
    s = [bigquery.SchemaField(k,t) for k,t in [("date","DATE"),("forecast_run_date","DATE"),
        ("city","STRING"),("state","STRING"),("lat","FLOAT"),("lng","FLOAT"),("temp_max","FLOAT"),
        ("feelslike_max","FLOAT"),("feelslike_min","FLOAT"),("humidity","FLOAT"),
        ("heat_risk","STRING"),("extreme_heat_threshold","FLOAT"),("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p)
    dedup = f"DELETE FROM `{p}.extremeheat_forecast_{e}.extremeheat_data` WHERE forecast_run_date='{rd}' AND city IS NOT NULL"
    tid = _write(c, p, f"extremeheat_forecast_{e}", "extremeheat_data", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} heat rows -> {tid}")

# 4b
def export_observed_temperature(grid, env_by_cell, manifest):
    """Observed (actual) temperature history → extremeheat_forecast_{env}.

    Sourced from the weather_history grid key (VC Timeline date-range pull).
    These are REAL past observations, keyed by their own date, with NO
    forecast_run_date — that's what distinguishes an actual from a forecast.
    Rows for today may be a nowcast; that's the intended actual/forecast
    handoff point. TRUNCATE: each run re-fetches the whole 45-day window.
    """
    p, e, r = _cfg()
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for lat, lng, cell, env in _grid_iter(grid, env_by_cell):
        hist = cell.get("weather_history", {})
        for d in (hist.get("days") or []):
            dt = d.get("datetime")
            if not dt:
                continue
            # guard: never let a forward statistical row leak in as "actual"
            if dt > today:
                continue
            rows.append({"date": dt, "city": env.get("aq_observed_city", ""),
                "state": env.get("aq_observed_state", ""), "lat": lat, "lng": lng,
                "actual_temp_max": _f(d.get("tempmax")),
                "actual_feelslike_max": _f(d.get("feelslikemax")),
                "actual_feelslike_min": _f(d.get("feelslikemin")),
                "actual_humidity": _f(d.get("humidity")),
                "record_inserted_at": now})
    if not rows:
        print("  BQ: 0 observed rows (no weather_history in grid)")
        return
    s = [bigquery.SchemaField(k, t) for k, t in [("date", "DATE"),
        ("city", "STRING"), ("state", "STRING"), ("lat", "FLOAT"), ("lng", "FLOAT"),
        ("actual_temp_max", "FLOAT"), ("actual_feelslike_max", "FLOAT"),
        ("actual_feelslike_min", "FLOAT"), ("actual_humidity", "FLOAT"),
        ("record_inserted_at", "TIMESTAMP")]]
    c = bigquery.Client(project=p)
    tid = _write(c, p, f"extremeheat_forecast_{e}", "observed_temperature",
                 s, rows, "WRITE_TRUNCATE", r)
    print(f"  BQ: {len(rows)} observed rows -> {tid}")


# 4c
def export_ili_forecast(env_by_cell, manifest):
    """ILI (flu-like illness) weekly forecast → ili_forecast_{env}.

    ILI is weekly and CBSA-regional, unlike the daily point hazards. This
    writes one row per (cell, epi_week) from the normalized ili_series the
    extractor produces — carrying numeric risk (secondary-axis metric),
    category (tooltip), and CBSA label. Only ili_usable cells are written,
    respecting the drift gate. TRUNCATE: latest forecast only.
    """
    p, e, r = _cfg()
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for v in env_by_cell.values():
        if v.get("ili_usable") is not True:
            continue
        series = v.get("ili_series") or []
        for (week_date, risk_num, risk_cat) in series:
            rows.append({
                "epi_week_start": week_date,
                "lat": _f(v.get("cell_lat")), "lng": _f(v.get("cell_lng")),
                "city": v.get("aq_observed_city", ""),
                "state": v.get("aq_observed_state", ""),
                "cbsa_id": v.get("ili_cbsa_id"),
                "cbsa_name": v.get("ili_cbsa_name"),
                "ili_risk": _i(risk_num),
                "ili_risk_category": risk_cat,
                "grid_drift_km": _f(v.get("ili_grid_drift_km")),
                "record_inserted_at": now})
    if not rows:
        print("  BQ: 0 ILI rows (none usable after drift gate)")
        return
    s = [bigquery.SchemaField(k, t) for k, t in [
        ("epi_week_start", "DATE"), ("lat", "FLOAT"), ("lng", "FLOAT"),
        ("city", "STRING"), ("state", "STRING"), ("cbsa_id", "STRING"),
        ("cbsa_name", "STRING"), ("ili_risk", "INTEGER"),
        ("ili_risk_category", "STRING"), ("grid_drift_km", "FLOAT"),
        ("record_inserted_at", "TIMESTAMP")]]
    c = bigquery.Client(project=p)
    tid = _write(c, p, f"ili_forecast_{e}", "ili_weekly_forecast",
                 s, rows, "WRITE_TRUNCATE", r)
    print(f"  BQ: {len(rows)} ILI rows -> {tid}")


# 5
def export_severe_weather(grid, env_by_cell, manifest):
    p, e, r = _cfg()
    now = datetime.now(timezone.utc).isoformat(); rd = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []
    for lat, lng, cell, env in _grid_iter(grid, env_by_cell):
        for d in (cell.get("weather",{}).get("days") or [])[:7]:
            dt=d.get("datetime");
            if not dt: continue
            rows.append({"date":dt,"forecast_run_date":rd,"city":env.get("aq_observed_city",""),
                "state":env.get("aq_observed_state",""),"lat":lat,"lng":lng,
                "precip_prob":d.get("precipprob"),"precip_inches":d.get("precip"),
                "precip_type":",".join(d.get("preciptype") or []),"snow":d.get("snow"),
                "wind_gust":d.get("windgust"),"severe_risk":d.get("severerisk"),
                "conditions":d.get("conditions"),"description":d.get("description"),
                "visibility":d.get("visibility"),"record_inserted_at":now})
    if not rows: return
    s = [bigquery.SchemaField(k,t) for k,t in [("date","DATE"),("forecast_run_date","DATE"),
        ("city","STRING"),("state","STRING"),("lat","FLOAT"),("lng","FLOAT"),
        ("precip_prob","FLOAT"),("precip_inches","FLOAT"),("precip_type","STRING"),("snow","FLOAT"),
        ("wind_gust","FLOAT"),("severe_risk","FLOAT"),("conditions","STRING"),("description","STRING"),
        ("visibility","FLOAT"),("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p)
    dedup = f"DELETE FROM `{p}.severe_weather_{e}.severe_weather_data` WHERE forecast_run_date='{rd}' AND city IS NOT NULL"
    tid = _write(c, p, f"severe_weather_{e}", "severe_weather_data", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} severe rows -> {tid}")

# 6
def export_wildfire_snapshot(env_by_cell, manifest):
    p, e, r = _cfg(); now = datetime.now(timezone.utc).isoformat()
    td = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = [{"date":td,"lat":_f(v.get("cell_lat")),"lng":_f(v.get("cell_lng")),
        "city":v.get("aq_observed_city",""),"state":v.get("aq_observed_state",""),
        "max_fwi":_f(v.get("max_fwi_in_radius")),"nearest_fire_km":_f(v.get("nearest_fire_km")),
        "days_since_last_fire":_i(v.get("days_since_last_fire")),
        "fire_count":_i(v.get("fire_count_in_radius")),"record_inserted_at":now}
        for v in env_by_cell.values()]
    s = [bigquery.SchemaField(k,t) for k,t in [("date","DATE"),("lat","FLOAT"),("lng","FLOAT"),
        ("city","STRING"),("state","STRING"),("max_fwi","FLOAT"),("nearest_fire_km","FLOAT"),
        ("days_since_last_fire","INTEGER"),("fire_count","INTEGER"),("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p)
    dedup = f"DELETE FROM `{p}.wildfire_forecast_{e}.wildfire_daily_snapshot` WHERE date='{td}'"
    tid = _write(c, p, f"wildfire_forecast_{e}", "wildfire_daily_snapshot", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} wildfire rows -> {tid}")

# 7
def export_aqi_snapshot(env_by_cell, manifest):
    p, e, r = _cfg(); now = datetime.now(timezone.utc).isoformat()
    td = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = [{"date":td,"lat":_f(v.get("cell_lat")),"lng":_f(v.get("cell_lng")),
        "city":v.get("aq_observed_city",""),"state":v.get("aq_observed_state",""),
        "aqi":_f(v.get("aqi")),"aqi_category":v.get("aqi_category"),
        "pm25":_f(v.get("pm25")),"pm10":_f(v.get("pm10")),
        "no2":_f(v.get("no2")),"ozone":_f(v.get("ozone")),"record_inserted_at":now}
        for v in env_by_cell.values()]
    s = [bigquery.SchemaField(k,t) for k,t in [("date","DATE"),("lat","FLOAT"),("lng","FLOAT"),
        ("city","STRING"),("state","STRING"),("aqi","FLOAT"),("aqi_category","STRING"),
        ("pm25","FLOAT"),("pm10","FLOAT"),("no2","FLOAT"),("ozone","FLOAT"),
        ("record_inserted_at","TIMESTAMP")]]
    c = bigquery.Client(project=p)
    dedup = f"DELETE FROM `{p}.airquality_forecast_{e}.aqi_daily_snapshot` WHERE date='{td}'"
    tid = _write(c, p, f"airquality_forecast_{e}", "aqi_daily_snapshot", s, rows, "WRITE_APPEND", r, dedup)
    print(f"  BQ: {len(rows)} AQI rows -> {tid}")

# All in one call
def export_all(scored, grid, env_by_cell, manifest):
    export_scored_patients(scored, manifest)
    export_daily_forecast(grid, env_by_cell, manifest)
    export_location_snapshot(env_by_cell, manifest)
    export_extremeheat(grid, env_by_cell, manifest)
    export_observed_temperature(grid, env_by_cell, manifest)
    export_ili_forecast(env_by_cell, manifest)
    export_severe_weather(grid, env_by_cell, manifest)
    export_wildfire_snapshot(env_by_cell, manifest)
    export_aqi_snapshot(env_by_cell, manifest)