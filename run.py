"""ERAAS Pipeline Orchestrator

Usage:  python run.py manifests/health_plan_b.yaml
"""
import json, sys, yaml
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
    print(f"\n{'='*68}\n{title}\n{'='*68}")


def main(manifest_path):
    manifest = yaml.safe_load(open(manifest_path))
    tid = manifest["tenant_id"]
    print(f"\n>>> ERAAS Pipeline: {manifest['display_name']} ({tid})")
    register_tenant(manifest)

    # STAGE 1
    hr("STAGE 1  LAND")
    datasets, written = land(manifest)
    for name, (n, path) in written.items():
        print(f"  {name:12s} {n:5d} rows -> {path}")
    raw_patients = pd.read_csv(manifest["source"]["path"]).to_dict("records")

    # STAGE 2
    hr("STAGE 2  ENRICH")
    grid, cells = enrich(manifest, raw_patients)
    prec = manifest["enrich"]["grid_precision"]
    lat_f = manifest["source"]["lat_field"]
    lng_f = manifest["source"]["lng_field"]
    env_by_cell = {}
    for key, cell_data in grid.items():
        if isinstance(key, tuple):
            lat, lng = key; str_key = f"{lat},{lng}"
        else:
            lat, lng = map(float, key.split(",")); str_key = key
        env_by_cell[str_key] = extract_all(cell_data, lat, lng, manifest["enrich"])
    print(f"  extracted {len(env_by_cell)} locations -> "
          f"{len(list(env_by_cell.values())[0])} fields each")

    # STAGE 3
    hr("STAGE 3  NORMALIZE")
    normalized = normalize_all(raw_patients, env_by_cell, manifest)
    print(f"  normalized {len(normalized)} records")
    print(f"  geo corrections: {sum(1 for r in normalized if r.get('geo_mismatch'))}/{len(normalized)}")

    # STAGE 3b
    hr("STAGE 3b  VALIDATE")
    valid, rejected = validate_batch(normalized)

    # STAGE 4
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
    print(f"  risk: {dict(risk)}")
    print(f"  drivers: {drivers.most_common(5)}")
    high = [p for p in scored if p["risk_category"] == "High"]
    print(f"\n  HIGH-RISK ({len(high)} -> nurse call list):")
    for p in sorted(high, key=lambda x: -(x.get("risk_score") or 0)):
        print(f"    {str(p.get('patient_id',''))[:20]:20s} "
              f"{str(p.get('first_name','')):10s} "
              f"age {str(p.get('age','?')):>3s} "
              f"{str(p.get('disease_names','')):12s} "
              f"{str(p.get('city','')):13s} -> "
              f"{str(p.get('driving_hazard',''))}")

    # STAGE 5
    hr("STAGE 5  LOAD (MongoDB)")
    db_name, collections = load(manifest, scored)

    # STAGE 5b
    hr("STAGE 5b  EXPORT (BigQuery)")
    try:
        from pipeline.export import export_all
        export_all(scored, grid, env_by_cell, manifest)
    except Exception as e:
        print(f"  BigQuery export failed: {e}")
        print(f"  (MongoDB load succeeded — BQ is non-blocking)")

    close()

    # SUMMARY
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