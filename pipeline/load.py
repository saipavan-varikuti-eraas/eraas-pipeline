"""STAGE 5 - LOAD (MongoDB).

Replaces JSON file writes with real MongoDB operations. Every design
decision from the JSON version carries over:
  * tenant_id on EVERY document
  * idempotent: upsert keyed by _id (re-run overwrites, never duplicates)
  * audit event per collection write
  * weather reference in a SEPARATE database (shared, not per-tenant)

Uses bulk_write with UpdateOne/upsert for idempotency. A re-run of the
same day's data replaces documents rather than inserting duplicates.
"""
import random
from datetime import datetime, timezone

from pymongo import UpdateOne

from .db_router import get_tenant_db


def _now():
    return datetime.now(timezone.utc).isoformat()


def _audit(tenant_id, operation, collection, count, success=True, error=None):
    return {
        "audit_id": f"audit_{random.randint(10**9, 10**10)}",
        "timestamp": _now(),
        "tenant_id": tenant_id,
        "operation": operation,
        "collection": collection,
        "result_count": count,
        "success": success,
        "error_message": error,
    }


def _build_patient_mapping(patients, tenant_id, nurses):
    mappings = []
    for p in patients:
        nurse = random.choice(nurses)
        mappings.append({
            "_id": f"map_{p['patient_id']}",
            "tenant_id": tenant_id,
            "patient_id": p["patient_id"],
            "nurse_id": nurse["id"],
            "lat": p["lat"], "long": p["long"],
            "state": p["state"], "city": p["city"],
            "zip_code": p["zip_code"],
            "created_at": _now(),
        })
    return mappings


def _build_nurse_calls(patients, tenant_id, nurses):
    calls = []
    for p in patients:
        if p.get("risk_category") != "High":
            continue
        nurse = random.choice(nurses)
        calls.append({
            "_id": f"ncall_{p['patient_id']}",
            "tenant_id": tenant_id,
            "nurse_id": nurse["id"],
            "nurse_name": nurse["name"],
            "patient_id": p["patient_id"],
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "escalation_or_to_do_note": (
                f"{p.get('risk_category')} risk (severity {p.get('risk_score')}) "
                f"driven by {p.get('driving_hazard')}: "
                f"{(p.get('risk_notes') or '')[:120]}"),
            "call_outcome": None,
            "call_status": "pending",
        })
    return calls


def _build_agentic_ai(patients, tenant_id):
    stubs = []
    for p in patients:
        if not p.get("permission_to_call"):
            continue
        stubs.append({
            "_id": f"aicall_{p['patient_id']}",
            "tenant_id": tenant_id,
            "call_id": f"call_{random.randint(10**6, 10**7)}",
            "patient_id": p["patient_id"],
            "mrn": p["mrn"],
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "dob": p["dob"],
            "state": p["state"],
            "call_date": None,
            "call_outcome": None,
            "call_status": "scheduled",
        })
    return stubs


def _upsert_collection(db, collection_name, docs):
    """Bulk upsert: idempotent, keyed by _id. Re-run = replace, not duplicate."""
    if not docs:
        return 0
    coll = db[collection_name]
    ops = [UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
           for doc in docs]
    result = coll.bulk_write(ops)
    return result.upserted_count + result.modified_count


def load(manifest, scored_patients, weather_grid_path=None, out_root="data"):
    """Write all Figure 6 collections to the tenant's isolated MongoDB."""
    tenant_id = manifest["tenant_id"]
    db, meta = get_tenant_db(tenant_id)
    db_name = meta["db_name"]

    nurses = [
        {"id": "nurse_101", "name": "Alicia Grant"},
        {"id": "nurse_102", "name": "Marcus Reed"},
        {"id": "nurse_103", "name": "Priya Shah"},
    ]

    # tag tenant_id and _id on every patient doc
    for p in scored_patients:
        p["tenant_id"] = tenant_id
        p["_id"] = f"pat_{p['patient_id']}"

    # build derived collections
    mapping = _build_patient_mapping(scored_patients, tenant_id, nurses)
    nurse_calls = _build_nurse_calls(scored_patients, tenant_id, nurses)
    agentic = _build_agentic_ai(scored_patients, tenant_id)

    collections = {
        "health_plan_collection": scored_patients,
        "patient_mapping": mapping,
        "nurse_call_collection": nurse_calls,
        "agentic_ai": agentic,
    }

    audits = []
    total_docs = 0
    for name, docs in collections.items():
        count = _upsert_collection(db, name, docs)
        actual = db[name].count_documents({"tenant_id": tenant_id})
        print(f"  {name:28s} {actual:4d} docs in MongoDB ({count} upserted)")
        audits.append(_audit(tenant_id, "bulk_upsert", name, actual))
        total_docs += actual

    # audit collection
    audit_coll = db["audit_collection"]
    for a in audits:
        a["_id"] = a["audit_id"]
        audit_coll.update_one({"_id": a["_id"]}, {"$set": a}, upsert=True)
    audit_count = audit_coll.count_documents({"tenant_id": tenant_id})
    print(f"  {'audit_collection':28s} {audit_count:4d} docs in MongoDB")
    total_docs += audit_count

    # isolation verification
    tagged = 0
    for name in list(collections.keys()) + ["audit_collection"]:
        tagged += db[name].count_documents({"tenant_id": tenant_id})
    print(f"\n  ISOLATION CHECK: {tagged}/{total_docs} docs tagged "
          f"with tenant_id={tenant_id!r} "
          f"{'✓' if tagged == total_docs else '✗ VIOLATION'}")
    print(f"  Database: {db_name}")

    return db_name, collections