"""Validation gate: the data contract between Normalize and Load.

Nothing reaches a tenant DB without passing these checks. The gate catches
SILENT wrongness - data that looks fine but isn't. Every rule here exists
because we found the violation in the real data:

  - adherence_score 588  (xlsx, should be 0-1)
  - risk_category uncorrelated with risk_score (xlsx)
  - state = FM (Micronesia) for a Chicago zip code (CSV)
  - age 31.997260273972604 (fractional derived value, xlsx)

A crash is obvious. A nurse dispatched to Micronesia is not.
"""


def _check(record, field, test, msg):
    """Returns (field, message) if test fails, else None."""
    val = record.get(field)
    if val is not None and not test(val):
        return (field, f"{field}={val!r}: {msg}")
    return None


def validate_patient(rec):
    """Returns (is_valid, errors: list[str], warnings: list[str])."""
    errors = []
    warnings = []

    # --- hard fails: record cannot be loaded ---
    if not rec.get("patient_id"):
        errors.append("patient_id is missing")
    if not rec.get("tenant_id"):
        errors.append("tenant_id is missing")
    if rec.get("lat") is None or rec.get("long") is None:
        errors.append("lat/long missing - cannot join weather")

    # adherence_score: PDC must be 0-1 (the xlsx had 588)
    a = rec.get("adherence_score")
    if a is not None and not (0 <= a <= 1.0):
        errors.append(f"adherence_score={a}: must be 0-1 (PDC). "
                      f"Value > 1 means the source is broken, not that "
                      f"the patient is super-adherent.")

    # age: must be plausible
    age = rec.get("age")
    if age is not None and not (0 <= age <= 120):
        errors.append(f"age={age}: implausible")

    # --- warnings: loadable but suspicious ---
    if rec.get("geo_mismatch"):
        warnings.append(
            f"state corrected: source had {rec.get('_source_state')!r}, "
            f"Ambee says {rec.get('state')!r}")

    if rec.get("disease_names") is None:
        warnings.append("disease_names is null - patient will score Low "
                        "(no condition to match against hazards)")

    if rec.get("dob") is None:
        warnings.append("dob missing - age cannot be computed")

    if rec.get("permission_to_call") is False:
        warnings.append("permission_to_call=False - no outreach allowed")

    return len(errors) == 0, errors, warnings


def validate_batch(records):
    """Validate a batch. Returns (valid, rejected, all_warnings)."""
    valid, rejected = [], []
    warn_count = 0
    for rec in records:
        ok, errs, warns = validate_patient(rec)
        if ok:
            rec["_validation_warnings"] = warns
            valid.append(rec)
            warn_count += len(warns)
        else:
            rec["_validation_errors"] = errs
            rejected.append(rec)

    print(f"  validation: {len(valid)} passed, {len(rejected)} rejected, "
          f"{warn_count} warnings")
    if rejected:
        for r in rejected[:5]:
            print(f"    REJECTED {r.get('patient_id')}: "
                  f"{'; '.join(r['_validation_errors'])}")
    return valid, rejected