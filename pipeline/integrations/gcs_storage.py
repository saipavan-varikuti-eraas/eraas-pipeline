"""GCS Landing Zone: replaces local source/ and data/raw/ with Cloud Storage.

Two jobs:
  1. READ raw source files from GCS (replaces local source/ folder)
  2. WRITE immutable raw copies to GCS (replaces data/raw/{tenant}/{date}/)

Uses the same bucket as the team's pipeline (ERAAS_RAW_BUCKET), organized
by tenant prefix:
  gs://eraas-rebuild-dev-raw/
    ├── airquality/          ← team's raw hazard CSVs
    ├── pollen/              ← team's raw hazard CSVs
    ├── health_plan_b/       ← OUR tenant's raw data
    │   └── 2026-08-06/
    │       └── patients.csv
    └── health_plan_c/       ← future tenants
"""
import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()


def _get_bucket():
    bucket_name = os.getenv("ERAAS_RAW_BUCKET")
    if not bucket_name:
        raise RuntimeError(
            "ERAAS_RAW_BUCKET not set. Add to .env: "
            "ERAAS_RAW_BUCKET=eraas-rebuild-dev-raw")
    client = storage.Client(project=os.getenv("ERAAS_PROJECT_ID"))
    return client.bucket(bucket_name)


def upload_raw(local_path, manifest):
    """Upload a local source file to GCS under the tenant's prefix.

    Path: gs://{bucket}/{tenant_id}/{date}/{filename}
    This is the 'immutable raw copy' — write-once, never edit.
    """
    tenant_id = manifest["tenant_id"]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = os.path.basename(local_path)
    gcs_path = f"{tenant_id}/{stamp}/{filename}"

    bucket = _get_bucket()
    blob = bucket.blob(gcs_path)

    # Don't overwrite if it already exists (immutability)
    if blob.exists():
        print(f"  GCS: {gcs_path} already exists (immutable, skipping)")
        return f"gs://{bucket.name}/{gcs_path}"

    blob.upload_from_filename(local_path)
    print(f"  GCS: {local_path} -> gs://{bucket.name}/{gcs_path}")
    return f"gs://{bucket.name}/{gcs_path}"


def read_csv_from_gcs(gcs_uri):
    """Read a CSV file directly from GCS into a DataFrame.

    Accepts gs://bucket/path/to/file.csv format.
    """
    # Parse gs:// URI
    path = gcs_uri.replace("gs://", "")
    bucket_name = path.split("/")[0]
    blob_path = "/".join(path.split("/")[1:])

    client = storage.Client(project=os.getenv("ERAAS_PROJECT_ID"))
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))
    return df


def read_excel_from_gcs(gcs_uri, sheet_name=0, header=0):
    """Read an Excel file from GCS."""
    path = gcs_uri.replace("gs://", "")
    bucket_name = path.split("/")[0]
    blob_path = "/".join(path.split("/")[1:])

    client = storage.Client(project=os.getenv("ERAAS_PROJECT_ID"))
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    content = blob.download_as_bytes()
    df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
    return df


def list_tenant_files(tenant_id, prefix=""):
    """List files in a tenant's GCS prefix. Useful for finding latest drop."""
    bucket = _get_bucket()
    full_prefix = f"{tenant_id}/{prefix}"
    blobs = list(bucket.list_blobs(prefix=full_prefix))
    return [b.name for b in blobs]


def land_to_gcs(manifest):
    """Upload the source file to GCS and return the GCS URI.

    This replaces the local-file land stage when source.gcs_path is set
    in the manifest. If source.path is local, it uploads first.
    """
    src = manifest["source"]

    # If already a GCS path, just return it
    if str(src.get("path", "")).startswith("gs://"):
        print(f"  GCS: source already in cloud: {src['path']}")
        return src["path"]

    # Local file → upload to GCS
    local_path = src["path"]
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Source file not found: {local_path}")

    gcs_uri = upload_raw(local_path, manifest)
    return gcs_uri