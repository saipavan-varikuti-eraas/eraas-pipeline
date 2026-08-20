"""BigQuery Dashboard Views: create SQL views for Looker Studio.

Views join scored patients with hazard data for richer dashboards.
Run once to create — views auto-update as underlying tables change.
"""
import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()


VIEWS = {
    # Patient risk dashboard — PHI-safe (uses hashed ID)
    "patient_risk_dashboard_view": """
        SELECT
            p.patient_id_hash,
            p.age,
            p.gender,
            p.disease_names,
            p.comorbidities,
            p.city,
            p.state,
            p.risk_score,
            p.risk_category,
            p.driving_hazard,
            p.risk_notes,
            p.permission_to_call,
            p.geo_mismatch,
            p.calculated_date,
            h.aqi,
            h.aqi_category,
            h.temperature,
            h.apparent_temperature,
            h.feelslike_max_forecast,
            h.heat_wave_expected,
            h.max_fwi_in_radius,
            h.pollen_risk_max,
            h.ili_risk_today
        FROM `{project}.patient_risk_{env}.scored_patients` p
        LEFT JOIN `{project}.weather_snapshot_{env}.location_hazards` h
            ON ROUND(p.lat, 2) = ROUND(h.cell_lat, 2)
            AND ROUND(p.lng, 2) = ROUND(h.cell_lng, 2)
            AND h.snapshot_date = p.calculated_date
    """,

    # Heat dashboard — all cities, 7-day forecast with threshold
    "heat_dashboard_view": """
        SELECT
            city,
            state,
            date,
            forecast_run_date,
            temp_max,
            feelslike_max,
            feelslike_min,
            humidity,
            heat_risk,
            extreme_heat_threshold,
            CASE
                WHEN feelslike_max >= 125 THEN 4
                WHEN feelslike_max >= 103 THEN 3
                WHEN feelslike_max >= 90 THEN 2
                WHEN feelslike_max >= 80 THEN 1
                ELSE 0
            END AS heat_severity
        FROM `{project}.extremeheat_forecast_{env}.extremeheat_data`
    """,

    # AQI dashboard — all cities, daily snapshot
    "aqi_dashboard_view": """
        SELECT
            city,
            state,
            date,
            aqi,
            aqi_category,
            pm25,
            pm10,
            no2,
            ozone,
            CASE
                WHEN aqi > 300 THEN 'Hazardous'
                WHEN aqi > 200 THEN 'Very Unhealthy'
                WHEN aqi > 150 THEN 'Unhealthy'
                WHEN aqi > 100 THEN 'Unhealthy for Sensitive'
                WHEN aqi > 50 THEN 'Moderate'
                ELSE 'Good'
            END AS epa_category,
            CASE
                WHEN aqi > 300 THEN 4
                WHEN aqi > 200 THEN 4
                WHEN aqi > 150 THEN 3
                WHEN aqi > 100 THEN 2
                WHEN aqi > 50 THEN 1
                ELSE 0
            END AS aqi_severity
        FROM `{project}.airquality_forecast_{env}.aqi_daily_snapshot`
    """,

    # Wildfire dashboard — all cities with risk classification
    "wildfire_dashboard_view": """
        SELECT
            city,
            state,
            date,
            max_fwi,
            nearest_fire_km,
            days_since_last_fire,
            fire_count,
            CASE
                WHEN max_fwi >= 38 THEN 'Very High / Extreme'
                WHEN max_fwi >= 21.3 THEN 'High'
                WHEN max_fwi >= 11.2 THEN 'Moderate'
                WHEN max_fwi >= 5.2 THEN 'Low'
                ELSE 'Very Low'
            END AS fwi_category,
            CASE
                WHEN max_fwi >= 38 THEN 4
                WHEN max_fwi >= 21.3 THEN 3
                WHEN max_fwi >= 11.2 THEN 2
                WHEN max_fwi >= 5.2 THEN 1
                ELSE 0
            END AS fwi_severity
        FROM `{project}.wildfire_forecast_{env}.wildfire_daily_snapshot`
    """,

    # Severe weather dashboard
    "severe_weather_dashboard_view": """
        SELECT
            city,
            state,
            date,
            severe_risk,
            precip_prob,
            precip_inches,
            precip_type,
            wind_gust,
            snow,
            conditions,
            description,
            visibility,
            CASE
                WHEN severe_risk >= 80 THEN 'Extreme'
                WHEN severe_risk >= 60 THEN 'High'
                WHEN severe_risk >= 30 THEN 'Moderate'
                ELSE 'Low'
            END AS severe_category
        FROM `{project}.severe_weather_{env}.severe_weather_data`
    """,

    # Combined hazard summary — one row per city per day
    "hazard_summary_view": """
        SELECT
            h.city,
            h.state,
            h.date AS forecast_date,
            h.feelslike_max AS heat_index,
            h.heat_risk,
            a.aqi,
            a.aqi_category,
            w.max_fwi,
            s.severe_risk,
            s.precip_prob,
            s.conditions,
            GREATEST(
                CASE WHEN h.feelslike_max >= 103 THEN 3 WHEN h.feelslike_max >= 90 THEN 2 ELSE 0 END,
                CASE WHEN a.aqi > 150 THEN 3 WHEN a.aqi > 100 THEN 2 ELSE 0 END,
                CASE WHEN w.max_fwi >= 38 THEN 3 WHEN w.max_fwi >= 21 THEN 2 ELSE 0 END,
                CASE WHEN s.severe_risk >= 60 THEN 3 WHEN s.severe_risk >= 30 THEN 2 ELSE 0 END
            ) AS max_hazard_severity
        FROM `{project}.extremeheat_forecast_{env}.extremeheat_data` h
        LEFT JOIN `{project}.airquality_forecast_{env}.aqi_daily_snapshot` a
            ON h.city = a.city AND h.date = a.date
        LEFT JOIN `{project}.wildfire_forecast_{env}.wildfire_daily_snapshot` w
            ON h.city = w.city AND h.date = w.date
        LEFT JOIN `{project}.severe_weather_{env}.severe_weather_data` s
            ON h.city = s.city AND h.date = s.date
    """,
}


def create_views():
    """Create all dashboard views in BigQuery."""
    project = os.getenv("ERAAS_PROJECT_ID")
    env = os.getenv("ERAAS_DATASET_ENV") or "dev"
    region = os.getenv("ERAAS_REGION", "us-east1")
    client = bigquery.Client(project=project)

    # Create a views dataset
    dataset_id = f"dashboard_views_{env}"
    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ds_ref.location = region
    client.create_dataset(ds_ref, exists_ok=True)

    for name, sql in VIEWS.items():
        view_id = f"{project}.{dataset_id}.{name}"
        formatted_sql = sql.format(project=project, env=env)

        view = bigquery.Table(view_id)
        view.view_query = formatted_sql
        try:
            client.delete_table(view_id, not_found_ok=True)
            client.create_table(view)
            print(f"  View: {view_id}")
        except Exception as e:
            print(f"  View FAILED {name}: {str(e)[:100]}")

    print(f"\n  All views in dataset: {dataset_id}")
    print(f"  Looker Studio: add data source -> BigQuery -> {dataset_id}")


if __name__ == "__main__":
    create_views()