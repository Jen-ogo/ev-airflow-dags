# ev-airflow-dags (project-only)

This repo contains **ONLY project Airflow DAGs + helper tools** for the EV geospatial / EV-charging data platform.

**Included**
- `dags/` — project DAGs as a Python package (Airflow reads these)
- `tools/` — small utilities (docs generator, helpers)

**Not included (by design)**
- any deployment files (`docker-compose.yml`, Airflow configs)
- any secrets / credentials (SMTP, Snowflake, Azure, EventHub, etc.)

---

## Structure

dags/
osm/        # Geofabrik OSM → extract layers → Parquet → ADLS (factory: DAG per country/region/dataset)
eurostat/   # Eurostat datasets → Parquet → ADLS
gisco/      # GISCO NUTS boundaries → Parquet → ADLS
traffic/    # TomTom traffic → EventHub (optional)
tools/
generate_dag_docs.py

> **Important:** DAGs use package imports (`ev_repository.dags.*`).  
> Keep `__init__.py` files in package folders so Airflow can import modules correctly.

---

## Mounting into Airflow

Mount the repository into Airflow’s DAGs folder so Airflow sees:

- `/opt/airflow/dags/ev_repository`

DAG files are then discovered from:

- `/opt/airflow/dags/ev_repository/dags/**`

If you see `ModuleNotFoundError`, it usually means the mount path is different than expected or `__init__.py` is missing.

---

## DAGs overview

### OSM (`dags/osm/`)
Downloads Geofabrik extracts, extracts dataset layers, transforms to Parquet, uploads to ADLS.  
Creates **one DAG per (country, region, dataset)** via a factory (`osm_layer_dag_factory.py`) and loader (`osm_dags.py`).  
ADLS output pattern: `raw/osm/<country>/<region>/<dataset>/dt=<ds>/`  
Uploads both:
- `{dataset}.parquet` (GeoParquet/WKB)
- `{dataset}_sf.parquet` (Snowflake-friendly, **geom_wkt text only**, no binary geometry)

### Eurostat (`dags/eurostat/`)
- `eurostat_tran_r_elvehst_full_to_adls`: full API JSON → tidy parquet → ADLS snapshot (`raw/eurostat/tran_r_elvehst/snapshot=<utc_ts>/`)
- `eurostat_degurba_lau_2021_etl`: DGURBA LAU zip → parquet with `geom_wkt` → ADLS (`raw/eurostat/degurba/lau/year=2021/`)
- `eurostat_population_grid_2021_europe`: population grid → GeoParquet + Snowflake-friendly parquet (`geom_wkt` in EPSG:4326) → ADLS (`raw/eurostat/population_grid/europe/census_grid_2021/`)

### GISCO (`dags/gisco/`)
- `gisco_nuts_2024_01m_4326_etl`: downloads NUTS GeoJSON for levels `[0,1,2,3]`, converts to Parquet with `geom_wkt` (no binary geom), validates schema, uploads to  
  `raw/gisco/nuts/year=2024/scale=01m/crs=4326/level=<n>/...parquet`

### EV availability (dags/streaming/)
- `tomtom_ev_enrichment_r7`: pulls top R7 candidates from Databricks (geo_databricks_sub.GOLD.FEAT_H3_MACRO_SCORE_R7) → calls TomTom POI Search (EV charging stations within a radius; radius is derived from kring_area_m2) → for each station with chargingAvailabilityId calls Charging Availability → writes enrichment into GOLD tables:
Snowflake, Databricks

### Traffic (`dags/traffic/`)
- `traffic_tomtom_ingestion`: scheduled every 10 minutes; loads latest candidate points snapshot from ADLS, optionally calls TomTom (or dry-run mock), pushes events to Azure EventHub, sends email. Behavior is controlled via Airflow Variables (e.g., `TRAFFIC_ENABLED`, `TOMTOM_DRY_RUN`, `EVENTHUB_ENABLED`).

---

## Requirements (runtime)
These DAGs assume the Airflow environment provides:
- Docker access for `DockerOperator` (and `docker.sock` mounted where needed)
- Python deps (installed in Airflow image): `azure-identity`, `azure-storage-blob`, `azure-eventhub` (+ Docker provider)

Secrets are supplied by your deployment (Airflow Connections/Variables/env), not by this repo.

---

## Docs generation
Generate a simple markdown index of all discovered DAGs:

```bash
python tools/generate_dag_docs.py
