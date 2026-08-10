# ERAAS Plan B Pipeline — Architecture Compliance & Findings

**Status:** Pipeline complete. All architecture components implemented.
**Last run:** 100 patients, 67 High / 26 Medium / 7 Low risk.

---

## Architecture Compliance

### Pipeline (all ✅)
| Stage | What | Output |
|---|---|---|
| 1. Land | CSV/XLSX → raw partitioned by tenant/date | `data/raw/` or GCS |
| 2. Enrich | Ambee (AQ, pollen, wildfire, ILI) + Visual Crossing (weather) | cached grid |
| 3. Normalize | Geo fix, type cast, derived field recompute | canonical patients |
| 3b. Validate | Data contract gate (adherence, age, geo, consent) | pass/reject |
| 4. Score | EPA AQI, NWS Heat Index, NWS Wind Chill, EFFIS FWI | risk + driver |
| 5. Load | MongoDB Atlas (5 Figure 6 collections, tenant-isolated) | `health_plan_b_db` |
| 5b. Export | BigQuery (scored_patients, location_hazards, daily_forecast) | Looker Studio |

### Infrastructure (all ✅)
| Component | Status |
|---|---|
| MongoDB Atlas | Connected, 5 collections, 327 docs tenant-tagged |
| DB Router | `tenants_metadata` collection, auto-registration |
| BigQuery | 3 tables in `eraas-rebuild-dev`, feeds Looker Studio |
| GCS | `gcs_storage.py` ready, same bucket as team (`eraas-rebuild-dev-raw`) |
| Redis cache | `redis_cache.py` ready, provider-scoped keys with TTL |
| Airflow | `eraas_dag_factory.py`, one manifest = one DAG, no per-tenant code |
| Integration | `enrich_mongo.py` reads from team's `eraas` db (1/10 coverage today) |

### Not in pipeline scope (backend/DevOps)
Go backend, JWT/RBAC, gRPC/Protobuf, REST API, Next.js frontend,
Docker/K8s/Helm, Prometheus/Grafana, ELK Stack.

---

## Data Quality Findings

**A1.** All defects are silent — nothing crashes. The validation gate catches what schemas can't.

**A2.** State/city are fiction in the source data (100/100 wrong, including Micronesia for Chicago).
Corrected from Ambee's reverse-geocode. Originals preserved in `_source_state`/`_source_city`.

**A3.** Incoming `risk_score`/`risk_category` randomly generated, 9+ months stale, no weather.
All derived fields recomputed by the pipeline.

**A4.** The xlsx (1000 patients) and CSVs (100 patients) are different populations, zero overlap.

**A5.** xlsx defects: `adherence_score` up to 599, `comorbidities` contains the string "comorbidities",
ICD-10 codes mismatched, three spellings of `long`/`lng`/`ling`.

**A6.** Four "hazard" CSVs are byte-identical patient rosters. Hazard values come from Ambee at runtime.

---

## Vendor Findings

**B1.** Ambee's air quality response includes city/state/zip — free reverse-geocode ground truth.

**B2.** HTTP 206 = quota exhausted, data trimmed, looks like success.

**B3.** ILI is Beta, coverage holes up to 568km. Gated at 50km with `ili_usable`/`ili_reject_reason`.

**B4.** `fire_count_in_radius` is vendor pagination (5 for every city including Manhattan).
`max_fwi_in_radius` is real signal. Flagged `fire_count_reliable: false`.

**B5.** Ambee weather = current hour only. Visual Crossing = 15-day forecast (why the team uses both).

**B6.** Weather APIs ≠ weather apps. Category-level consistency is what matters.

---

## Bugs Found and Fixed

| Bug | Impact | Fix |
|---|---|---|
| Heat flag used dry-bulb temp, not heat index | Philadelphia 110.9°F flagged safe | Use `feelslikemax` |
| Half-fix: flag corrected, date not | Confident wrong day for nurse | Flag + date from same row |
| `provider: ambee` was decoration | Not a real config dial | `providers.py` registry |
| Cache key missing provider | Ambee JSON fed to VC extractor → silent nulls | Provider in key |
| Cache key change invalidated cache | 40 unplanned API calls | Production: coordinate deploys |
| ILI nulled without reason | "No data" = "low risk" confusion | `ili_usable` + `ili_reject_reason` |

---

## Architecture Principles

1. Weather is a property of a place, not a person (100 patients → 10 locations, 90% fewer calls)
2. Land raw, unchanged (audit evidence + reprocessing capability)
3. Config vs code (client facts → manifest; `if tenant ==` in `pipeline/` = broken)
4. Secrets ≠ config ≠ code (key → .env/Vault; endpoint → manifest; logic → code)
5. Fetch faithfully, interpret later (5 envelopes, 4 shapes, reshaping is separate)
6. Max severity, not average (each alert cites specific hazard + authority)
7. Test one call before looping
8. Adapters absorb source variation; providers absorb vendor variation
9. Reject with a reason, never silently null
10. Uniform values = smell; extreme values ≠ wrong

---

## Open Questions

1. Which patient population is canonical — xlsx (1000) or CSVs (100)?
2. Where are the hazard output files the Data Dictionary describes?
3. NWS Wind Chill thresholds are locally set — current bands are national defaults.
4. FWI classes use EFFIS (European). US regional calibrations may differ.
5. NWS/CDC HeatRisk is the upgrade path (purpose-built, location-adjusted).
6. Second tenant build needed to prove manifest pattern end to end.

---

## File Inventory

```
eraas-pipeline/
├── run.py                          # orchestrator
├── eraas_dag_factory.py            # Airflow DAG factory
├── manifests/
│   └── health_plan_b.yaml          # one file per tenant
├── pipeline/
│   ├── __init__.py
│   ├── land.py                     # Stage 1: CSV/XLSX adapters
│   ├── providers.py                # vendor auth abstraction
│   ├── enrich.py                   # Stage 2: API fetch, deduped, cached
│   ├── enrich_mongo.py             # Stage 2 alt: read from team's eraas db
│   ├── hazards.py                  # Ambee extractors
│   ├── hazards_vc.py               # Visual Crossing extractor
│   ├── extract_registry.py         # (hazard, provider) → extractor
│   ├── normalize.py                # Stage 3: geo fix, type cast
│   ├── validate.py                 # Stage 3b: data contract gate
│   ├── standards.py                # EPA/NWS/EFFIS thresholds
│   ├── score.py                    # Stage 4: condition × hazard
│   ├── mongo_client.py             # MongoDB connection
│   ├── db_router.py                # DB Router (tenants metadata)
│   ├── load.py                     # Stage 5: MongoDB upsert
│   ├── export_bq.py                # Stage 5b: BigQuery export
│   ├── export_forecast.py          # 7-day forecast to BigQuery
│   ├── gcs_storage.py              # GCS landing zone
│   └── redis_cache.py              # Redis weather cache
├── source/                         # local source files (gitignored)
├── data/                           # local output (gitignored)
├── .env                            # secrets (gitignored)
├── .gitignore
└── ERAAS_PIPELINE_FINDINGS.md
```