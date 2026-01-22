# dags/osm/osm_dags.py
# DAG loader only (no heavy logic here)

from ev_repository.dags.osm.osm_config import COUNTRIES_REGIONS, ENABLED_DATASETS
from ev_repository.dags.osm.osm_layer_dag_factory import build_osm_layer_dag


# Airflow discovers DAG objects from this module via globals().
for country, region in COUNTRIES_REGIONS:
    for dataset in ENABLED_DATASETS:
        dag = build_osm_layer_dag(country=country, region=region, dataset=dataset)
        globals()[dag.dag_id] = dag