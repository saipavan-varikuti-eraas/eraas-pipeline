"""STAGE 4 - SCORE: clinical condition x environmental hazard.

DESIGN (confirmed with the team, 2026-07-16):
  * Use OFFICIAL government standards (EPA / NWS / EFFIS) - never invented weights.
  * Take the MAX severity across the hazards a patient is sensitive to.
    NOT an average. Chicago's AQI 335 must not be diluted by "pollen is Low".
    This mirrors how the agencies work: EPA does not average AQI with pollen;
    NWS does not average heat with wind chill. They issue a SPECIFIC alert for
    a SPECIFIC hazard - and so do we.
  * Escalate one level for the standard's own "sensitive group" definition.
    EPA literally names AQI 101-150 "Unhealthy for Sensitive Groups" and
    defines those as heart/lung disease + older adults. The standard performs
    the condition x hazard match for us.
  * Every score names the DRIVING HAZARD and cites its authority, so a nurse
    knows what to say on the call.

The architecture document specifies NO formula - it names risk_score,
risk_category and risk_notes but contains no weights or thresholds. This
implementation therefore leans entirely on published standards rather than
engineering guesswork. See ERAAS_PIPELINE_FINDINGS.md section E5.
"""
from datetime import date

from .standards import (
    classify_aqi, classify_heat_index, classify_wind_chill, classify_fwi,
    classify_pollen, classify_ili, SEVERITY_NAME,
    CONDITION_SENSITIVITY, EPA_SENSITIVE_CONDITIONS, EPA_SENSITIVE_AGE,
    CDC_HEAT_SENSITIVE_CONDITIONS, CDC_HEAT_SENSITIVE_AGE,
    COMORBIDITY_AMPLIFIERS,
)

RISK_CATEGORY = {0: "Low", 1: "Low", 2: "Medium", 3: "High", 4: "High"}


def _hazard_severities(env):
    """Classify every hazard present at this location, via official standards."""
    out = {}

    cat, sev = classify_aqi(env.get("aqi"))
    if sev is not None:
        out["air_quality"] = {
            "value": env.get("aqi"), "category": cat, "severity": sev,
            "authority": "EPA AQI",
            "detail": f"AQI {env.get('aqi')} ({cat})"}

    # heat/cold use FORECAST feels-like: the point is to warn BEFORE it lands
    fl_max = env.get("feelslike_max_forecast")
    cat, sev = classify_heat_index(fl_max)
    if sev is not None:
        out["heat"] = {
            "value": fl_max, "category": cat, "severity": sev,
            "authority": "NWS Heat Index",
            "detail": f"heat index {fl_max}F ({cat}) by {env.get('hottest_day')}"}

    fl_min = env.get("feelslike_min_forecast")
    cat, sev = classify_wind_chill(fl_min)
    if sev is not None:
        out["cold"] = {
            "value": fl_min, "category": cat, "severity": sev,
            "authority": "NWS Wind Chill",
            "detail": f"wind chill {fl_min}F ({cat}) by {env.get('coldest_day')}"}

    # NOTE: max_fwi_in_radius only. fire_count_in_radius is vendor pagination
    # (Ambee returns top-N regardless of distance) - never score on it.
    fwi = env.get("max_fwi_in_radius")
    cat, sev = classify_fwi(fwi)
    if sev is not None:
        out["wildfire"] = {
            "value": fwi, "category": cat, "severity": sev,
            "authority": "FWI (EFFIS)",
            "detail": f"FWI {round(fwi,1)} ({cat}), nearest fire "
                      f"{env.get('nearest_fire_km')}km"}

    cat, sev = classify_pollen(env.get("pollen_risk_max"))
    if sev is not None:
        out["pollen"] = {
            "value": cat, "category": cat, "severity": sev,
            "authority": "Ambee pollen risk",
            "detail": f"pollen {cat}"}

    # ILI only if it survived the drift gate (Ambee Beta snaps up to 568km)
    if env.get("ili_usable"):
        cat, sev = classify_ili(env.get("ili_risk_peak")
                               or env.get("ili_risk_today"))
        if sev is not None:
            out["ili"] = {
                "value": cat, "category": cat, "severity": sev,
                "authority": "Ambee ILI forecast (Beta)",
                "detail": f"ILI {cat}"}
    return out


def _patient_sensitivities(condition, comorbidity, age):
    """Which hazards threaten THIS patient, and where they get escalated."""
    hazards = set(CONDITION_SENSITIVITY.get(condition, []))
    escalate = {}

    if condition in EPA_SENSITIVE_CONDITIONS:
        hazards.add("air_quality")
        escalate.setdefault("air_quality", []).append(
            f"{condition} (EPA sensitive group: heart/lung disease)")
    if age is not None and age >= EPA_SENSITIVE_AGE:
        hazards.add("air_quality")
        escalate.setdefault("air_quality", []).append(
            f"age {age} (EPA sensitive group: older adult)")

    if condition in CDC_HEAT_SENSITIVE_CONDITIONS:
        hazards.add("heat")
        escalate.setdefault("heat", []).append(
            f"{condition} (CDC heat-vulnerable)")
    if age is not None and age >= CDC_HEAT_SENSITIVE_AGE:
        hazards.add("heat")
        escalate.setdefault("heat", []).append(
            f"age {age} (CDC heat-vulnerable)")

    if comorbidity:
        for hz in COMORBIDITY_AMPLIFIERS.get(comorbidity, []):
            hazards.add(hz)
            escalate.setdefault(hz, []).append(f"{comorbidity} amplifies {hz}")

    return hazards, escalate


def score_patient(patient, env, cfg=None):
    """Score one patient against one location's environment."""
    condition = patient.get("disease_names")
    comorbidity = patient.get("comorbidities")
    age = patient.get("age")

    all_hz = _hazard_severities(env)
    sensitive_to, escalations = _patient_sensitivities(condition, comorbidity, age)

    assessed = {}
    for hz, info in all_hz.items():
        if hz not in sensitive_to:
            continue
        sev = info["severity"]
        reasons = escalations.get(hz, [])
        if reasons and sev > 0:          # escalate only a real hazard, never 0
            sev = min(sev + 1, 4)
        assessed[hz] = {**info, "adjusted_severity": sev,
                        "escalated_because": reasons}

    if not assessed:
        return {
            "risk_score": 0, "risk_category": "Low", "driving_hazard": None,
            "risk_notes": (f"No hazards assessed: condition {condition!r} has "
                           f"no environmental sensitivities, or no usable "
                           f"hazard data at this location."),
            "calculated_date": date.today().isoformat(),
            "hazard_detail": {},
        }

    driving = max(assessed.items(), key=lambda kv: kv[1]["adjusted_severity"])
    hz_name, hz = driving
    sev = hz["adjusted_severity"]

    note = (f"{hz['detail']} [{hz['authority']}: {hz['category']}]. "
            f"Patient has {condition}")
    if hz["escalated_because"]:
        note += f"; escalated to {SEVERITY_NAME[sev]} - " + \
                "; ".join(hz["escalated_because"])
    else:
        note += f"; severity {SEVERITY_NAME[sev]}"
    others = [f"{k}={v['category']}" for k, v in assessed.items() if k != hz_name]
    if others:
        note += f". Also assessed: {', '.join(others)}"

    return {
        "risk_score": sev,
        "risk_category": RISK_CATEGORY[sev],
        "driving_hazard": hz_name,
        "risk_notes": note,
        "calculated_date": date.today().isoformat(),
        "hazard_detail": assessed,
    }