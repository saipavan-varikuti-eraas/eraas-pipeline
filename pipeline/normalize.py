"""STAGE 3 - NORMALIZE: clean patient records into canonical shape.

Three jobs:
  1. Fix geography: overwrite junk state/city from Ambee's reverse-geocode,
     keep originals for audit (confirmed decision 2026-07-16).
  2. Recompute derived fields: age, comorbidities count. Never trust incoming
     derived values - the xlsx had adherence_score=588 and risk_category
     uncorrelated with risk_score.
  3. Cast types: everything landed as strings (Step 1 discipline); now we
     parse to the types the schema needs.
"""
from datetime import date, datetime


def _parse_age(dob_str):
    """Recompute age from DOB. Never trust an incoming age field."""
    if not dob_str:
        return None
    try:
        if isinstance(dob_str, (int, float)):
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                d = datetime.strptime(str(dob_str).strip(), fmt).date()
                today = date.today()
                return today.year - d.year - (
                    (today.month, today.day) < (d.month, d.day))
            except ValueError:
                continue
    except Exception:
        pass
    return None


def _safe_float(v):
    try:
        return float(v) if v is not None and str(v).strip() != "" else None
    except (ValueError, TypeError):
        return None


def _safe_int(v):
    try:
        return int(float(v)) if v is not None and str(v).strip() != "" else None
    except (ValueError, TypeError):
        return None


def normalize_patient(raw, env, tenant_id):
    """One raw patient dict -> one canonical patient dict."""
    out = {}

    # --- identity (pass-through) ---
    out["tenant_id"] = tenant_id
    out["patient_id"] = raw.get("patient_id")
    out["mrn"] = raw.get("mrn")
    out["first_name"] = raw.get("first_name")
    out["last_name"] = raw.get("last_name")
    out["dob"] = str(raw.get("dob", ""))[:10] if raw.get("dob") else None
    out["gender"] = raw.get("gender")
    out["race_ethnicity"] = raw.get("race_ethnicity")
    out["email"] = raw.get("email")
    out["phone_number1"] = str(raw.get("phone_number1", "")) or None
    out["phone_number2"] = str(raw.get("phone_number2", "")) or None
    out["phone_number3"] = str(raw.get("phone_number3", "")) or None
    out["street_address"] = raw.get("street_address")
    out["zip_code"] = str(raw.get("zip_code", "")) or None

    # --- geography: Ambee is authoritative (confirmed decision) ---
    out["lat"] = _safe_float(raw.get("lat"))
    out["long"] = _safe_float(raw.get("lng") or raw.get("long"))
    out["_source_state"] = raw.get("state")
    out["_source_city"] = raw.get("city")
    out["city"] = env.get("aq_observed_city") or raw.get("city")
    out["state"] = env.get("aq_observed_state") or raw.get("state")
    out["geo_mismatch"] = (raw.get("state") != out["state"])

    # --- clinical ---
    out["disease_names"] = raw.get("disease_names")
    out["icd_10_codes"] = raw.get("icd_10_codes")
    out["comorbidities"] = raw.get("comorbidities")
    out["hospitalization_reason"] = raw.get("hospitalization_reason")
    out["medical_notes"] = raw.get("medical_notes")
    out["medication_names"] = raw.get("medication_names")
    out["dosages"] = raw.get("dosages")
    out["frequency"] = raw.get("frequency")
    out["last_refill_date"] = raw.get("last_refill_date")
    out["pharmacy_notes"] = raw.get("pharmacy_notes")
    out["permission_to_call"] = str(raw.get("permission_to_call", "")).strip().lower() in (
        "true", "yes", "y", "1")
    out["purpose_of_call"] = raw.get("purpose_of_call")

    # --- recomputed derived fields (NEVER trust incoming) ---
    out["age"] = _parse_age(raw.get("dob"))
    out["adherence_score"] = _safe_float(raw.get("adherence_score"))
    out["recent_hospitalization_rate"] = _safe_float(
        raw.get("recent_hospitalization_rate"))

    # risk_score, risk_category, risk_notes, calculated_date -> set by scorer
    # incoming values are discarded on purpose (see findings doc A3)
    out["risk_score"] = None
    out["risk_category"] = None
    out["risk_notes"] = None
    out["calculated_date"] = None

    return out


def normalize_all(raw_patients, env_by_cell, manifest):
    """Normalize all patients, joining each to their weather cell."""
    tenant_id = manifest["tenant_id"]
    prec = manifest["enrich"]["grid_precision"]
    lat_f = manifest["source"]["lat_field"]
    lng_f = manifest["source"]["lng_field"]

    normalized = []
    for raw in raw_patients:
        key = f"{round(float(raw[lat_f]), prec)},{round(float(raw[lng_f]), prec)}"
        env = env_by_cell.get(key, {})
        normalized.append(normalize_patient(raw, env, tenant_id))
    return normalized