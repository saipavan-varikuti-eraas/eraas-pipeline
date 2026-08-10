# ERAAS: Health Plan B Data Pipeline

Environmental Risk as a Service — a care management pipeline that combines patient health data with environmental hazard data to generate risk scores for proactive nurse outreach.

## What This Does

Takes 100 patients with chronic conditions across 10 US cities. Fetches live environmental data (air quality, pollen, wildfire, weather, influenza) for each location. Scores every patient against their local hazards using published government standards. Produces a prioritized nurse call list — **who to call today, and why.**

```
100 patients → 10 locations → 5 hazard APIs → risk scored → nurse call list

Latest run (2026-08-10):
  76 High risk | 16 Medium | 8 Low
  Top drivers: heat (68), wildfire (16), air quality (9)
  76 nurse tasks auto-generated
  55 AI call stubs scheduled (consented patients)
```

## Pipeline Stages

```
Stage 1: LAND        Raw CSV → partitioned by tenant/date (immutable)
Stage 2: ENRICH      Ambee (AQ/pollen/wildfire/ILI) + Visual Crossing (weather)
                     100 patients → 10 locations = 90% fewer API calls
Stage 3: NORMALIZE   Geography corrected from Ambee reverse-geocode (100/100 fixed)
                     Derived fields recomputed (never trust incoming risk_score)
Stage 3b: VALIDATE   Data contract gate — adherence 0-1, age plausible, geo verified
Stage 4: SCORE       EPA AQI + NWS Heat Index + NWS Wind Chill + EFFIS FWI
                     MAX severity, not average — each score names the driving hazard
Stage 5: LOAD        MongoDB Atlas (5 collections, tenant-isolated, 374 docs verified)
Stage 5b: EXPORT     BigQuery (3 tables → Looker Studio dashboards)
```

## Architecture

```
Patient CSV
  → pipeline (this repo)
    → MongoDB Atlas (care management app)
    → BigQuery (Looker Studio dashboards)

Team's hazard pipeline (Apache Beam / Dataflow)
  → BigQuery (hazard forecasts)
  → MongoDB 'eraas' db
    ← this pipeline can read from it (enrich_mongo integration)
```

### Multi-Tenant Design

One codebase, many tenants. Each tenant is a YAML manifest — onboarding client #6 is writing a config file, not new code.

```
manifests/
  health_plan_b.yaml     ← tenant config
  health_plan_c.yaml     ← next client (same pipeline, different data)
```

The DB Router auto-registers tenants in a `tenants_metadata` MongoDB collection and routes writes to the correct database (dedicated or shared isolation strategy).

### Risk Scoring

Every threshold traces to a published government standard:

| Hazard | Standard | Source |
|---|---|---|
| Air Quality | EPA AQI | airnow.gov |
| Heat | NWS Heat Index | weather.gov (uses feels-like, not dry-bulb) |
| Cold | NWS Wind Chill | weather.gov |
| Wildfire | FWI danger classes | EFFIS |
| Pollen | Ambee risk bands | getambee.com |
| ILI | Ambee forecast | getambee.com (Beta, drift-gated at 50km) |

Condition × hazard matching: a COPD patient in Chicago (AQI 335) and a CHF patient in Phoenix (heat index 112°F) are both High risk **for different reasons**, needing different nurse conversations.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill in your keys
python run.py manifests/health_plan_b.yaml
```

### Required in `.env`

```
AMBEE_API_KEY=
VISUAL_CROSSING_API_KEY=
MONGO_URI=mongodb+srv://...
ERAAS_PROJECT_ID=eraas-rebuild-dev
ERAAS_REGION=us-east1
ERAAS_RAW_BUCKET=eraas-rebuild-dev-raw
ERAAS_DATASET_ENV=dev
REDIS_URL=redis://localhost:6379/0
```

### Dependencies

```
pip install pandas pyyaml faker requests python-dotenv pymongo[srv] certifi
pip install google-cloud-bigquery google-cloud-storage redis
```

## Project Structure

```
eraas-pipeline/
├── run.py                          # Orchestrator (any tenant, one command)
├── eraas_dag_factory.py            # Airflow DAG factory
├── manifests/
│   └── health_plan_b.yaml          # One file per tenant
├── pipeline/
│   ├── land.py                     # Stage 1: CSV/XLSX adapters
│   ├── enrich.py                   # Stage 2: multi-vendor fetch, cached
│   ├── extractors.py               # Vendor response → canonical fields
│   ├── normalize.py                # Stage 3: geo fix, type cast, recompute
│   ├── validate.py                 # Stage 3b: data contract gate
│   ├── standards.py                # EPA/NWS/EFFIS thresholds
│   ├── score.py                    # Stage 4: condition × hazard scoring
│   ├── load.py                     # Stage 5: MongoDB (tenant-isolated)
│   ├── export.py                   # Stage 5b: BigQuery (Looker Studio)
│   ├── mongo_client.py             # MongoDB connection helper
│   ├── db_router.py                # Tenant routing via metadata service
│   └── integrations/
│       ├── enrich_mongo.py         # Read hazards from team's eraas db
│       ├── gcs_storage.py          # GCS landing zone
│       └── redis_cache.py          # Redis weather cache
├── source/                         # Raw input files (gitignored)
├── data/                           # Pipeline output (gitignored)
├── ERAAS_PIPELINE_FINDINGS.md      # Architecture decisions + discoveries
└── .env                            # Secrets (gitignored)
```

## BigQuery Tables

| Dataset | Table | Description |
|---|---|---|
| `patient_risk_dev` | `scored_patients` | 100 scored patient records (PHI hashed) |
| `weather_snapshot_dev` | `daily_forecast` | 7-day weather forecast per location |
| `weather_snapshot_dev` | `location_hazards` | Daily environmental snapshot |

## Key Decisions (see ERAAS_PIPELINE_FINDINGS.md)

- **Weather is a property of a place, not a person** — dedup patients to locations before API calls
- **Land raw, unchanged** — never clean on ingest; raw is the only undo button
- **Config vs code** — client facts live in the manifest, never in `if tenant ==` branches
- **Max severity, not average** — Chicago's AQI 335 must not be diluted by "pollen Low"
- **Heat index, not dry-bulb** — Philadelphia at 110.9°F feels-like was missed by dry-bulb; fixed
- **Reject with a reason** — ILI drift > 50km returns `ili_usable: false` + stated reason, not silent null
- **fire_count_in_radius is vendor pagination**, not signal — use max_fwi_in_radius instead

## MongoDB Collections (Figure 6)

```
health_plan_b_db/
├── health_plan_collection    100 patient records (scored)
├── patient_mapping           100 patient → nurse → geography
├── nurse_call_collection      76 High-risk outreach tasks
├── agentic_ai                 55 AI call stubs (consented)
└── audit_collection           pipeline operation log
```

All documents carry `tenant_id`. Isolation verified: 374/374 docs tagged.