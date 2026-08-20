"""GCS Storage: upload raw input + scored output to Cloud Storage.

Bucket layout (matches team's pattern):
  gs://eraas-rebuild-dev-raw/
    ├── health_plan_b/raw/2026-08-20/patients.csv      ← raw input (immutable)
    ├── health_plan_b/scored/2026-08-20/scored.csv      ← scored output
    └── health_plan_b/scored/2026-08-20/hazards.csv     ← location hazards
"""
import os
import json
import csv
import io
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()


def _get_bucket():
    bucket_name = os.getenv("ERAAS_RAW_BUCKET")
    if not bucket_name:
        raise RuntimeError("ERAAS_RAW_BUCKET not set in .env")
    client = storage.Client(project=os.getenv("ERAAS_PROJECT_ID"))
    return client.bucket(bucket_name)


def upload_raw(manifest):
    """Upload the raw source CSV to GCS. Immutable — skip if exists."""
    tenant_id = manifest["tenant_id"]
    local_path = manifest["source"]["path"]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = os.path.basename(local_path)
    gcs_path = f"{tenant_id}/raw/{stamp}/{filename}"

    bucket = _get_bucket()
    blob = bucket.blob(gcs_path)
    if blob.exists():
        print(f"  GCS: {gcs_path} already exists (immutable, skipped)")
        return f"gs://{bucket.name}/{gcs_path}"

    blob.upload_from_filename(local_path)
    print(f"  GCS: uploaded raw -> gs://{bucket.name}/{gcs_path}")
    return f"gs://{bucket.name}/{gcs_path}"


def upload_scored(scored_patients, env_by_cell, manifest):
    """Upload scored patients + hazards as CSVs to GCS."""
    tenant_id = manifest["tenant_id"]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bucket = _get_bucket()

    # Scored patients CSV
    if scored_patients:
        exclude = {"hazard_detail", "_validation_warnings"}
        keys = [k for k in scored_patients[0].keys() if k not in exclude]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(scored_patients)

        path = f"{tenant_id}/scored/{stamp}/scored_patients.csv"
        blob = bucket.blob(path)
        blob.upload_from_string(buf.getvalue(), content_type="text/csv")
        print(f"  GCS: uploaded scored -> gs://{bucket.name}/{path}")

    # Location hazards CSV
    if env_by_cell:
        env_list = list(env_by_cell.values())
        keys = list(env_list[0].keys())
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(env_list)

        path = f"{tenant_id}/scored/{stamp}/location_hazards.csv"
        blob = bucket.blob(path)
        blob.upload_from_string(buf.getvalue(), content_type="text/csv")
        print(f"  GCS: uploaded hazards -> gs://{bucket.name}/{path}")