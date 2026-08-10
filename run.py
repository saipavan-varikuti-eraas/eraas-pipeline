"""ERAAS Pipeline Orchestrator

Usage:  python run.py manifests/health_plan_b.yaml

Runs all stages for any tenant manifest. This file is the same for every
tenant — the manifest is the only thing that differs between clients.
"""
import json
import sys
import yaml
import pandas as pd
from collections import Counter

from pipeline.land import land
from pipeline.enrich import enrich
from pipeline.extractors import extract_all
from pipeline.normalize import normalize_all
from pipeline.validate import validate_batch
from pipeline.score import score_patient
from pipeline.db_router import register_tenant
from pipeline.load import load
from pipeline.mongo_client import close


def hr(title):
    print(f"\n{'=' * 68}\n{title}\n{'=' * 68}")


def main(manifest_path):
    manifest = yaml.safe_load(open(manifest_path))
    tid = manifest["tenant_id"]
    print(f"\n>>> ERAAS Pipeline: {manifest['display_name']} ({tid})")

    # Register tenant in metadata service
    register_tenant(manifest)

    # --- STAGE 1: LAND ---
    hr("STAGE 1  LAND")
    datasets, written = land(manifest)
    for name, (n, path) in written.items():
        print(f"  {name:12s} {n:5d} rows -> {path}")

    raw_patients = pd.read_csv(manifest["source"]["path"]).to_dict("records")

    # --- STAGE 2: ENRICH ---
    hr("STAGE 2  ENRICH")
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
    print(f"  extracted {len(env_by_cell)} locations -> "
          f"{len(list(env_by_cell.values())[0])} canonical fields each")

    # --- STAGE 3: NORMALIZE ---
    hr("STAGE 3  NORMALIZE")
    normalized = normalize_all(raw_patients, env_by_cell, manifest)
    print(f"  normalized {len(normalized)} patient records")
    geo_fixes = sum(1 for r in normalized if r.get("geo_mismatch"))
    print(f"  geography corrections: {geo_fixes}/{len(normalized)}")

    # --- STAGE 3b: VALIDATE ---
    hr("STAGE 3b  VALIDATE")
    valid, rejected = validate_batch(normalized)

    # --- STAGE 4: SCORE ---
    hr("STAGE 4  SCORE")
    scored = []
    for patient, raw in zip(valid, raw_patients[:len(valid)]):
        key = f"{round(float(raw[lat_f]), prec)},{round(float(raw[lng_f]), prec)}"
        env = env_by_cell.get(key, {})
        result = score_patient(patient, env)
        patient.update(result)
        scored.append(patient)

    risk = Counter(p["risk_category"] for p in scored)
    drivers = Counter(p["driving_hazard"] for p in scored if p.get("driving_hazard"))
    print(f"  risk distribution: {dict(risk)}")
    print(f"  top drivers: {drivers.most_common(5)}")

    high = [p for p in scored if p["risk_category"] == "High"]
    print(f"\n  HIGH-RISK ({len(high)} -> nurse call list):")
    for p in sorted(high, key=lambda x: -(x.get("risk_score") or 0)):
        print(f"    {str(p.get('patient_id',''))[:20]:20s} "
              f"{str(p.get('first_name','')):10s} "
              f"age {str(p.get('age','?')):>3s} "
              f"{str(p.get('disease_names','')):12s} "
              f"{str(p.get('city','')):13s} -> "
              f"{str(p.get('driving_hazard',''))}")

    # --- STAGE 5: LOAD (MongoDB) ---
    hr("STAGE 5  LOAD")
    db_name, collections = load(manifest, scored)

    # --- STAGE 5b: EXPORT (BigQuery) ---
    hr("STAGE 5b  BIGQUERY EXPORT")
    try:
        from pipeline.export import (export_scored_patients,
                                     export_daily_forecast,
                                     export_location_snapshot)
        export_scored_patients(scored, manifest)
        export_daily_forecast(grid, env_by_cell, manifest)
        export_location_snapshot(env_by_cell, manifest)
    except Exception as e:
        print(f"  BigQuery export failed: {e}")
        print(f"  (MongoDB load succeeded — BQ is non-blocking)")

    # --- CLEANUP ---
    close()

    # --- SUMMARY ---
    hr("PIPELINE COMPLETE")
    print(f"  tenant:      {tid}")
    print(f"  patients:    {len(scored)} scored, {len(rejected)} rejected")
    print(f"  risk:        {dict(risk)}")
    print(f"  nurse tasks: {len(collections['nurse_call_collection'])}")
    print(f"  AI calls:    {len(collections['agentic_ai'])}")
    print(f"  output:      {db_name}/")
    print(f"\n  To re-run: python run.py {manifest_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py manifests/health_plan_b.yaml")
        sys.exit(1)
    main(sys.argv[1])