"""Airflow DAG Factory: one manifest = one DAG. No per-tenant DAG files.

Drop a manifest YAML in the manifests/ folder, Airflow discovers it and
creates a scheduled DAG automatically. Onboarding tenant #6 = one YAML file.

Setup:
  pip install apache-airflow
  Set AIRFLOW__CORE__DAGS_FOLDER to include this file's directory
  (or symlink this file into your existing dags/ folder)

How it works:
  1. Scans manifests/ for *.yaml files
  2. For each manifest, creates an Airflow DAG with:
     - Schedule from the manifest's schedule.cron field
     - Tasks for each pipeline stage (land, enrich, normalize, score, load)
     - Error handling and retries
  3. Each DAG is independent — tenant A's failure doesn't block tenant B

In Cloud Composer (GCP managed Airflow):
  Upload manifests to GCS: gs://composer-bucket/dags/manifests/
  Upload this file to: gs://composer-bucket/dags/eraas_dag_factory.py
  Composer discovers both and creates the DAGs automatically.
"""
import os
import glob
from datetime import datetime, timedelta

import yaml

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Where manifests live (relative to this file)
MANIFEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifests")

DEFAULT_ARGS = {
    "owner": "eraas-pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _run_pipeline(manifest_path, **kwargs):
    """Execute the full pipeline for one tenant. Called by Airflow."""
    # Import here to avoid loading pipeline code at DAG parse time
    import yaml as _yaml
    from pipeline.land import land
    from pipeline.enrich import enrich
    from pipeline.extract_registry import extract_all
    from pipeline.normalize import normalize_all
    from pipeline.validate import validate_batch
    from pipeline.score import score_patient
    from pipeline.load import load
    from pipeline.db_router import register_tenant
    from pipeline.mongo_client import close
    import pandas as pd

    manifest = _yaml.safe_load(open(manifest_path))
    tenant_id = manifest["tenant_id"]
    print(f">>> ERAAS Pipeline: {manifest['display_name']} ({tenant_id})")

    # Register tenant
    register_tenant(manifest)

    # Stage 1: Land
    datasets, written = land(manifest)
    raw_patients = pd.read_csv(manifest["source"]["path"]).to_dict("records")

    # Stage 2: Enrich
    grid, cells = enrich(manifest, raw_patients)
    prec = manifest["enrich"]["grid_precision"]
    lat_f = manifest["source"]["lat_field"]
    lng_f = manifest["source"]["lng_field"]
    env_by_cell = {}
    for key, cell_data in grid.items():
        if isinstance(key, tuple):
            lat, lng = key
            str_key = f"{lat},{lng}"
        else:
            lat, lng = map(float, key.split(","))
            str_key = key
        env_by_cell[str_key] = extract_all(cell_data, lat, lng, manifest["enrich"])

    # Stage 3: Normalize + Validate
    normalized = normalize_all(raw_patients, env_by_cell, manifest)
    valid, rejected = validate_batch(normalized)

    # Stage 4: Score
    scored = []
    for patient, raw in zip(valid, raw_patients[:len(valid)]):
        key = f"{round(float(raw[lat_f]), prec)},{round(float(raw[lng_f]), prec)}"
        env = env_by_cell.get(key, {})
        result = score_patient(patient, env)
        patient.update(result)
        scored.append(patient)

    # Stage 5: Load (MongoDB)
    load(manifest, scored)

    # Stage 5b: BigQuery export
    try:
        from pipeline.export_bq import export_to_bigquery
        from pipeline.export_forecast import export_daily_forecast
        export_to_bigquery(scored, env_by_cell, manifest)
        export_daily_forecast(grid, env_by_cell, manifest)
    except Exception as e:
        print(f"BigQuery export failed (non-blocking): {e}")

    close()

    from collections import Counter
    risk = Counter(p["risk_category"] for p in scored)
    print(f"Complete: {len(scored)} scored, {len(rejected)} rejected, risk={dict(risk)}")
    return {"scored": len(scored), "rejected": len(rejected), "risk": dict(risk)}


def create_dag(manifest_path):
    """Create an Airflow DAG from a manifest YAML."""
    with open(manifest_path) as f:
        manifest = yaml.safe_load(f)

    tenant_id = manifest["tenant_id"]
    display_name = manifest.get("display_name", tenant_id)
    schedule = manifest.get("schedule", {}).get("cron", None)

    dag_id = f"eraas_{tenant_id}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"ERAAS pipeline for {display_name}",
        schedule_interval=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["eraas", tenant_id],
        max_active_runs=1,  # one run at a time per tenant
    )

    run_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=_run_pipeline,
        op_kwargs={"manifest_path": manifest_path},
        dag=dag,
    )

    return dag


# --- DAG Factory: scan manifests/ and create one DAG per YAML ---
# This block runs at DAG parse time (every ~30s in Airflow)
if AIRFLOW_AVAILABLE and os.path.isdir(MANIFEST_DIR):
    for manifest_file in glob.glob(os.path.join(MANIFEST_DIR, "*.yaml")):
        try:
            dag = create_dag(manifest_file)
            # Register the DAG in the global namespace so Airflow finds it
            dag_name = f"eraas_{os.path.basename(manifest_file).replace('.yaml', '')}"
            globals()[dag_name] = dag
        except Exception as e:
            print(f"Failed to create DAG from {manifest_file}: {e}")


# --- For local testing without Airflow ---
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python eraas_dag_factory.py manifests/health_plan_b.yaml")
        print("(runs the pipeline directly, without Airflow)")
        sys.exit(1)

    manifest_path = sys.argv[1]
    if not os.path.exists(manifest_path):
        print(f"Manifest not found: {manifest_path}")
        sys.exit(1)

    print("Running pipeline directly (no Airflow)...")
    result = _run_pipeline(manifest_path)
    print(f"\nResult: {result}")