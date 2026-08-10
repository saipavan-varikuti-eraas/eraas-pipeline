"""DB Router — resolves tenant_id to a database handle.

Implements the routing flow from Figure 4 (Sequence Diagram):
  1. Pipeline provides tenant_id
  2. Router queries tenants_metadata.tenants for isolation_strategy + db_reference
  3. Returns a database handle to the correct MongoDB database

Supports two isolation strategies (Figure 3):
  - "dedicated": tenant gets its own database (the "Large tenant" approach)
  - "shared": tenant's data lives in a shared database, isolated by tenant_id
    on every document (the "Small/Medium" approach)

The router also handles tenant registration: on first run for a new tenant,
it creates the metadata entry from the manifest. This is the onboarding path.
"""
from datetime import datetime, timezone

from .mongo_client import get_database, get_client

METADATA_DB = "tenants_metadata"
TENANTS_COLLECTION = "tenants"


def _metadata_collection():
    return get_database(METADATA_DB)[TENANTS_COLLECTION]


def register_tenant(manifest):
    """Register or update a tenant in the metadata service.

    Called at pipeline start. Idempotent: re-running updates, never duplicates.
    This is the 'onboarding a new client = one manifest + one run' promise.
    """
    tenant_id = manifest["tenant_id"]
    doc = {
        "_id": tenant_id,
        "tenant_id": tenant_id,
        "display_name": manifest.get("display_name", tenant_id),
        "isolation_strategy": "dedicated",       # ERAAS chose Large tenant
        "db_name": f"{tenant_id}_db",
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "status": "active",
        "source_type": manifest.get("source", {}).get("type"),
        "hazards": list(manifest.get("enrich", {}).get("hazards", {}).keys()),
    }

    coll = _metadata_collection()
    coll.update_one({"_id": tenant_id}, {"$set": doc}, upsert=True)
    print(f"  tenant registered: {tenant_id} -> {doc['db_name']} "
          f"({doc['isolation_strategy']})")
    return doc


def get_tenant_db(tenant_id):
    """The DB Router: resolve tenant_id -> database handle.

    This is the core of Figure 4's sequence:
      tenant_id -> tenants_metadata lookup -> isolation_strategy + db_reference
      -> return the correct database handle.
    """
    coll = _metadata_collection()
    meta = coll.find_one({"_id": tenant_id})

    if meta is None:
        raise ValueError(
            f"Tenant {tenant_id!r} not found in {METADATA_DB}.{TENANTS_COLLECTION}. "
            f"Run register_tenant() first — this happens automatically in run.py.")

    if meta.get("status") != "active":
        raise ValueError(
            f"Tenant {tenant_id!r} is {meta.get('status')!r}, not 'active'. "
            f"Pipeline will not write to an inactive tenant.")

    strategy = meta["isolation_strategy"]
    db_name = meta["db_name"]

    if strategy == "dedicated":
        # Large tenant: own database, no tenant_id filter needed on queries
        # (but we still TAG every doc with tenant_id for portability)
        db = get_database(db_name)
    elif strategy == "shared":
        # Small/Medium tenant: shared database, MUST filter by tenant_id
        db = get_database(db_name)
    else:
        raise ValueError(f"Unknown isolation_strategy {strategy!r} for {tenant_id}")

    return db, meta


def list_tenants():
    """List all registered tenants. Useful for monitoring / admin."""
    coll = _metadata_collection()
    tenants = list(coll.find({}))
    return tenants