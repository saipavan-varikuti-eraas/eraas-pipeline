"""Vendor response → canonical hazard fields.

One extractor per (hazard, provider) pair. Ambee weather and Visual Crossing
weather are both "weather" but share no structure — the extractor is chosen
by the pair, not the hazard name alone.

Every extractor is total: returns the same keys even when data is missing,
so downstream never guesses whether a field exists.

SOURCES: Ambee (AQ, pollen, wildfire, ILI), Visual Crossing (weather/forecast).
"""
from datetime import datetime, timezone, date
from math import radians, cos, sin, asin, sqrt

_RISK_ORDER = ["Low", "Moderate", "High", "Very High"]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat, dlng = radians(lat2 - lat1), radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlng/2)**2
    return 2 * R * asin(sqrt(a))


# ---------------------------------------------------------------------------
# Ambee extractors
# ---------------------------------------------------------------------------
def _air_quality(resp, lat, lng, cfg):
    """Envelope: {message, stations:[...]}. Array may be empty or multi."""
    out = {"aqi": None, "aqi_category": None, "pm25": None, "pm10": None,
           "no2": None, "ozone": None, "so2": None, "co": None,
           "aq_observed_city": None, "aq_observed_state": None,
           "aq_observed_zip": None, "aq_station_count": 0,
           "aq_updated_at": None}
    stations = (resp or {}).get("stations") or []
    out["aq_station_count"] = len(stations)
    if not stations:
        return out
    s = stations[0]
    info = s.get("aqiInfo") or {}
    out.update({
        "aqi": s.get("AQI"), "aqi_category": info.get("category"),
        "pm25": s.get("PM25"), "pm10": s.get("PM10"), "no2": s.get("NO2"),
        "ozone": s.get("OZONE"), "so2": s.get("SO2"), "co": s.get("CO"),
        "aq_observed_city": s.get("city"), "aq_observed_state": s.get("state"),
        "aq_observed_zip": s.get("postalCode"), "aq_updated_at": s.get("updatedAt"),
    })
    return out


def _pollen(resp, lat, lng, cfg):
    """Envelope: {message, lat, lng, data:[{Risk{}, Count{}, Species{}}]}."""
    out = {"grass_pollen_risk": None, "tree_pollen_risk": None,
           "weed_pollen_risk": None, "grass_pollen_count": None,
           "tree_pollen_count": None, "weed_pollen_count": None,
           "pollen_risk_max": None}
    rows = (resp or {}).get("data") or []
    if not rows:
        return out
    r0 = rows[0]
    risk, count = r0.get("Risk") or {}, r0.get("Count") or {}
    out.update({
        "grass_pollen_risk": risk.get("grass_pollen"),
        "tree_pollen_risk": risk.get("tree_pollen"),
        "weed_pollen_risk": risk.get("weed_pollen"),
        "grass_pollen_count": count.get("grass_pollen"),
        "tree_pollen_count": count.get("tree_pollen"),
        "weed_pollen_count": count.get("weed_pollen"),
    })
    present = [v for v in risk.values() if v in _RISK_ORDER]
    if present:
        out["pollen_risk_max"] = max(present, key=_RISK_ORDER.index)
    return out


def _weather_ambee(resp, lat, lng, cfg):
    """Envelope: {message, data:{...}} — an OBJECT, not a list."""
    out = {"temperature": None, "apparent_temperature": None, "humidity": None,
           "uv_index": None, "wind_speed": None, "precip_probability": None,
           "weather_summary": None, "weather_updated_at": None}
    d = (resp or {}).get("data") or {}
    if not isinstance(d, dict) or not d:
        return out
    out.update({
        "temperature": d.get("temperature"),
        "apparent_temperature": d.get("apparentTemperature"),
        "humidity": d.get("humidity"), "uv_index": d.get("uvIndex"),
        "wind_speed": d.get("windSpeed"),
        "precip_probability": d.get("precipProbability"),
        "weather_summary": d.get("summary"),
        "weather_updated_at": d.get("updatedAt"),
    })
    return out


def _wildfire(resp, lat, lng, cfg):
    """Envelope: {message, data:[fires]} — each fire at its own coords."""
    radius = cfg.get("wildfire_radius_km", 50)
    out = {"fire_count_in_radius": 0, "fire_count_reliable": False,
           "nearest_fire_km": None, "nearest_fire_fwi": None,
           "max_fwi_in_radius": None, "max_frp_in_radius": None,
           "days_since_last_fire": None, "wildfire_radius_km": radius}
    fires = (resp or {}).get("data") or []
    scored = []
    for f in fires:
        if f.get("lat") is None or f.get("lng") is None:
            continue
        d = haversine_km(lat, lng, f["lat"], f["lng"])
        if d <= radius:
            scored.append((d, f))
    if not scored:
        return out

    scored.sort(key=lambda t: t[0])
    out["fire_count_in_radius"] = len(scored)
    out["fire_count_reliable"] = False  # Ambee returns top-N, not filtered
    out["nearest_fire_km"] = round(scored[0][0], 2)
    out["nearest_fire_fwi"] = scored[0][1].get("fwi")

    fwis = [f.get("fwi") for _, f in scored if f.get("fwi") is not None]
    frps = [f.get("frp") for _, f in scored if f.get("frp") is not None]
    out["max_fwi_in_radius"] = max(fwis) if fwis else None
    out["max_frp_in_radius"] = max(frps) if frps else None

    dates = []
    for _, f in scored:
        if f.get("detectedAt"):
            dates.append(datetime.fromisoformat(
                f["detectedAt"].replace("Z", "+00:00")))
    if dates:
        out["days_since_last_fire"] = (datetime.now(timezone.utc) - max(dates)).days
    return out


# Numeric <-> category mapping so both Ambee schemas normalize to the same
# fields. Old schema gives a category string ("Low"); new schema gives an int
# ili_risk (1..) plus ili_risk_category. We always emit BOTH.
_ILI_CAT_TO_NUM = {"Low": 1, "Moderate": 2, "High": 3, "Very High": 4}
_ILI_NUM_TO_CAT = {v: k for k, v in _ILI_CAT_TO_NUM.items()}


def _ili_detect_format(resp, rows):
    """Return 'new' (weekly CBSA / epi_week / int risk) or 'old' (daily /
    createdAt / string risk). Ambee migrated schemas ~Aug 2026; cache and
    possibly live responses contain both, so the extractor handles either."""
    if not rows:
        return "empty"
    r0 = rows[0]
    if "epi_week_start_date" in r0 or resp.get("resolution") == "CBSA":
        return "new"
    if "createdAt" in r0:
        return "old"
    return "unknown"


def _ili_series_new(resp, rows):
    """Normalize new weekly CBSA format -> list of (date, num, category)."""
    series = []
    for r in rows:
        ds = r.get("epi_week_start_date")
        if not ds:
            continue
        dt = datetime.fromisoformat(ds.replace("Z", "+00:00"))
        num = r.get("ili_risk")
        cat = r.get("ili_risk_category") or _ILI_NUM_TO_CAT.get(num)
        if num is None and cat in _ILI_CAT_TO_NUM:
            num = _ILI_CAT_TO_NUM[cat]
        series.append((dt, num, cat))
    return series


def _ili_series_old(rows):
    """Normalize old daily string format -> list of (date, num, category)."""
    series = []
    for r in rows:
        ds = r.get("createdAt")
        risk = r.get("ili_risk")
        if not ds or risk is None:
            continue
        dt = datetime.fromisoformat(ds.replace("Z", "+00:00"))
        # old risk is a category string
        cat = risk if isinstance(risk, str) else _ILI_NUM_TO_CAT.get(risk)
        num = _ILI_CAT_TO_NUM.get(cat) if cat else (risk if isinstance(risk, int) else None)
        series.append((dt, num, cat))
    return series


def _ili(resp, lat, lng, cfg):
    """Ambee ILI — handles BOTH the old daily/string schema and the new
    weekly CBSA/int schema. Emits canonical scalar fields for the location
    snapshot AND the full normalized weekly series (ili_series) for the
    time-series export. Keeps the 50km drift gate; carries CBSA when present.
    """
    horizon = cfg.get("ili_horizon_days", 14)
    max_drift = cfg.get("ili_max_drift_km", 50)
    out = {"ili_risk_today": None, "ili_risk_today_num": None,
           "ili_risk_peak": None, "ili_peak_date": None,
           "ili_horizon_days": horizon, "ili_grid_drift_km": None,
           "ili_usable": None, "ili_reject_reason": None,
           "ili_cbsa_id": None, "ili_cbsa_name": None,
           "ili_schema": None, "ili_series": []}
    resp = resp or {}
    rows = resp.get("data") or []
    fmt = _ili_detect_format(resp, rows)
    out["ili_schema"] = fmt
    if fmt in ("empty", "unknown"):
        if fmt == "unknown":
            out["ili_reject_reason"] = "unrecognized ILI response schema"
        return out

    # CBSA label (new schema only carries it explicitly)
    out["ili_cbsa_id"] = resp.get("cbsa_id")
    out["ili_cbsa_name"] = resp.get("cbsa_name")

    # ---- drift gate --------------------------------------------------------
    # New schema: coords at top level. Old schema: coords repeated per row.
    ref_lat = resp.get("lat")
    ref_lng = resp.get("lng")
    if ref_lat is None and rows:
        ref_lat, ref_lng = rows[0].get("lat"), rows[0].get("lng")
    if ref_lat is not None and ref_lng is not None:
        drift = round(haversine_km(lat, lng, ref_lat, ref_lng), 2)
        out["ili_grid_drift_km"] = drift
        if drift > max_drift:
            out["ili_usable"] = False
            out["ili_reject_reason"] = f"grid drift {drift}km > {max_drift}km limit"
            return out
        out["ili_usable"] = True
    else:
        # no coords to check — mark usable but note the gate couldn't run
        out["ili_usable"] = True
        out["ili_reject_reason"] = "no coords in response; drift gate skipped"

    # ---- normalize series --------------------------------------------------
    series = _ili_series_new(resp, rows) if fmt == "new" else _ili_series_old(rows)
    series = [(d, n, c) for d, n, c in series if n is not None]
    if not series:
        return out
    series.sort()

    # expose the full series for the export (ISO date, num, category)
    out["ili_series"] = [(d.date().isoformat(), n, c) for d, n, c in series]

    now = datetime.now(timezone.utc)
    past_or_now = [(d, n, c) for d, n, c in series if d <= now]
    current = past_or_now[-1] if past_or_now else series[0]
    out["ili_risk_today"] = current[2]
    out["ili_risk_today_num"] = current[1]

    within = [(d, n, c) for d, n, c in series if 0 <= (d - now).days <= horizon]
    if within:
        peak = max(within, key=lambda t: t[1])
        out["ili_risk_peak"] = peak[2]
        out["ili_peak_date"] = peak[0].date().isoformat()
    return out


# ---------------------------------------------------------------------------
# Visual Crossing extractor
# ---------------------------------------------------------------------------
def _weather_vc(resp, lat, lng, cfg):
    """VC returns history + 15-day forecast in one call."""
    horizon = cfg.get("weather_horizon_days", 7)
    cold_thr = cfg.get("cold_threshold_f", 32)
    heat_thr = cfg.get("heat_threshold_f", 100)

    out = {
        "temperature": None, "apparent_temperature": None, "humidity": None,
        "uv_index": None, "wind_speed": None, "weather_summary": None,
        "temp_min_forecast": None, "temp_max_forecast": None,
        "feelslike_min_forecast": None, "feelslike_max_forecast": None,
        "coldest_day": None, "hottest_day": None,
        "cold_snap_expected": None, "heat_wave_expected": None,
        "days_to_cold_snap": None, "days_to_heat_wave": None,
        "weather_horizon_days": horizon, "weather_resolved_address": None,
    }
    if not resp:
        return out

    out["weather_resolved_address"] = resp.get("resolvedAddress")
    cur = resp.get("currentConditions") or {}
    out.update({
        "temperature": cur.get("temp"),
        "apparent_temperature": cur.get("feelslike"),
        "humidity": cur.get("humidity"),
        "uv_index": cur.get("uvindex"),
        "wind_speed": cur.get("windspeed"),
        "weather_summary": cur.get("conditions"),
    })

    days = resp.get("days") or []
    today = date.today()
    fc = []
    for d in days:
        if not d.get("datetime"):
            continue
        try:
            dt = datetime.strptime(d["datetime"], "%Y-%m-%d").date()
        except ValueError:
            continue
        delta = (dt - today).days
        if 0 <= delta <= horizon:
            fc.append((dt, delta, d))
    if not fc:
        return out

    mins = [(d.get("tempmin"), dt, delta) for dt, delta, d in fc
            if d.get("tempmin") is not None]
    maxs = [(d.get("tempmax"), dt, delta) for dt, delta, d in fc
            if d.get("tempmax") is not None]
    fl_mins = [(d.get("feelslikemin"), dt, delta) for dt, delta, d in fc
               if d.get("feelslikemin") is not None]
    fl_maxs = [(d.get("feelslikemax"), dt, delta) for dt, delta, d in fc
               if d.get("feelslikemax") is not None]

    if mins:
        out["temp_min_forecast"] = min(mins, key=lambda t: t[0])[0]
    if maxs:
        out["temp_max_forecast"] = max(maxs, key=lambda t: t[0])[0]

    if fl_mins:
        v, dt, delta = min(fl_mins, key=lambda t: t[0])
        out["feelslike_min_forecast"] = v
        out["coldest_day"] = dt.isoformat()
        out["cold_snap_expected"] = v <= cold_thr
        out["days_to_cold_snap"] = delta if v <= cold_thr else None

    if fl_maxs:
        v, dt, delta = max(fl_maxs, key=lambda t: t[0])
        out["feelslike_max_forecast"] = v
        out["hottest_day"] = dt.isoformat()
        out["heat_wave_expected"] = v >= heat_thr
        out["days_to_heat_wave"] = delta if v >= heat_thr else None

    return out


# ---------------------------------------------------------------------------
# Registry: (hazard, provider) → extractor
# ---------------------------------------------------------------------------
EXTRACTORS = {
    ("air_quality", "ambee"): _air_quality,
    ("pollen", "ambee"): _pollen,
    ("wildfire", "ambee"): _wildfire,
    ("ili", "ambee"): _ili,
    ("weather", "ambee"): _weather_ambee,
    ("weather", "visual_crossing"): _weather_vc,
}


def extract_all(cell_responses, lat, lng, cfg):
    """Flatten one location's responses into canonical fields."""
    flat = {"cell_lat": lat, "cell_lng": lng}
    for hazard, resp in cell_responses.items():
        # weather_history is raw time-series consumed directly by
        # export_observed_temperature off the grid — it has no canonical
        # per-location fields to flatten, so skip the extractor dispatch.
        if hazard == "weather_history":
            continue
        provider = cfg["hazards"][hazard]["provider"]
        fn = EXTRACTORS.get((hazard, provider))
        if fn is None:
            raise KeyError(
                f"no extractor for hazard={hazard!r} provider={provider!r}. "
                f"registered: {sorted(EXTRACTORS)}")
        flat.update(fn(resp, lat, lng, cfg))
    return flat