from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from docker.types import Mount

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient


# ============================================================
# CONFIG
# ============================================================
YEAR = "2021"
SCOPE = "europe"
DATASET = f"census_grid_{YEAR}"

DOWNLOAD_URLS = [
    "https://gisco-services.ec.europa.eu/census/2021/Eurostat_Census-GRID_2021_V2.2.zip",
    "https://gisco-services.ec.europa.eu/census/2021/Eurostat_Census-GRID_2021_V2.1.zip",
    "https://gisco-services.ec.europa.eu/census/2021/Eurostat_Census-GRID_2021_V2-0.zip",
]

# Host mount (Mac)
HOST_DATA_DIR = "/Volumes/T7/airflow-docker/data"

BASE_DIR = f"/data/eurostat/population_grid/{SCOPE}/{DATASET}"
DOWNLOAD_DIR = f"{BASE_DIR}/00_download"
TRANSFORM_DIR = f"{BASE_DIR}/02_transform"
LOCAL_TRANSFORM_DIR = (
    f"/opt/airflow/data/eurostat/population_grid/{SCOPE}/{DATASET}/02_transform"
)

ZIP_FILE = f"{DOWNLOAD_DIR}/{DATASET}.zip"
UNZIP_DIR = f"{DOWNLOAD_DIR}/01_unzipped"

# We know exact layer + geom column from your ogrinfo
PARQUET_LAYER = "ESTAT_Census_2021_V2"
GEOM_COL = "geom"

# Source file name in ZIP (as in your folder screenshot)
SRC_PARQUET_IN_ZIP = f"{UNZIP_DIR}/{PARQUET_LAYER}.parquet"

# Outputs
GEO_OUT = f"{TRANSFORM_DIR}/{DATASET}.parquet"        # GeoParquet with WKB geometry (EPSG:4326)
SF_OUT = f"{TRANSFORM_DIR}/{DATASET}_sf.parquet"     # Snowflake-friendly (GEOMETRY=NONE + geom_wkt)

# Snowflake projection (explicit fields only)
FIELDS = ",".join(
    [
        "GRD_ID",
        "T",
        "M",
        "F",
        "Y_LT15",
        "Y_1564",
        "Y_GE65",
        "EMP",
        "NAT",
        "EU_OTH",
        "OTH",
        "SAME",
        "CHG_IN",
        "CHG_OUT",
        "T_CI",
        "M_CI",
        "F_CI",
        "Y_LT15_CI",
        "Y_1564_CI",
        "Y_GE65_CI",
        "EMP_CI",
        "NAT_CI",
        "EU_OTH_CI",
        "OTH_CI",
        "SAME_CI",
        "CHG_IN_CI",
        "CHG_OUT_CI",
        "LAND_SURFACE",
        "POPULATED",
    ]
)

# ADLS
ADLS_CONTAINER = "uc-root"
ADLS_BASE_PATH = "raw/eurostat/population_grid"
REMOTE_PREFIX = f"{ADLS_BASE_PATH}/{SCOPE}/{DATASET}/"

EMAIL_TO = ["coach.tutor1993@gmail.com"]


def bash(cmd: str):
    # IMPORTANT: do not manually escape quotes; let bash handle them
    return ["/bin/bash", "-lc", cmd]


common_mounts = [Mount(source=HOST_DATA_DIR, target="/data", type="bind")]


def load_to_adls_gen2() -> None:
    storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]
    account_url = f"https://{storage_account}.blob.core.windows.net"

    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    service = BlobServiceClient(account_url, credential=credential)
    container = service.get_container_client(ADLS_CONTAINER)

    # wipe only this dataset prefix
    for blob in container.list_blobs(name_starts_with=REMOTE_PREFIX):
        if blob.name.endswith(".parquet"):
            container.delete_blob(blob.name)

    # upload parquet outputs
    for fn in os.listdir(LOCAL_TRANSFORM_DIR):
        if fn.endswith(".parquet"):
            with open(os.path.join(LOCAL_TRANSFORM_DIR, fn), "rb") as fh:
                container.upload_blob(REMOTE_PREFIX + fn, fh, overwrite=True)


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id=f"eurostat_population_grid_{YEAR}_{SCOPE}",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["eurostat", "gisco", "population_grid", "epsg3035", "epsg4326"],
) as dag:

    clean_workspace = DockerOperator(
        task_id="clean_workspace",
        image="osm-tools:latest",
        auto_remove=True,
        mounts=common_mounts,
        mount_tmp_dir=False,
        command=bash(
            f"""
            set -euo pipefail
            rm -rf "{DOWNLOAD_DIR}" "{TRANSFORM_DIR}"
            mkdir -p "{DOWNLOAD_DIR}" "{TRANSFORM_DIR}"
            """
        ),
    )

    download_zip = DockerOperator(
        task_id="download_zip",
        image="osm-tools:latest",
        auto_remove=True,
        mounts=common_mounts,
        mount_tmp_dir=False,
        command=bash(
            f"""
            set -euo pipefail
            ok=0
            for url in {' '.join([f'"{u}"' for u in DOWNLOAD_URLS])}; do
              echo "Trying: $url"
              if curl -fL --retry 5 --retry-delay 10 --retry-connrefused "$url" -o "{ZIP_FILE}"; then
                ok=1
                break
              fi
            done
            if [ "$ok" -ne 1 ]; then
              echo "ERROR: all download URLs failed" >&2
              exit 22
            fi
            ls -lh "{ZIP_FILE}" | sed -n '1,50p'
            """
        ),
    )

    check_zip = DockerOperator(
        task_id="check_zip",
        image="osm-tools:latest",
        auto_remove=True,
        mounts=common_mounts,
        mount_tmp_dir=False,
        command=bash(
            f"""
            set -euo pipefail
            test -f "{ZIP_FILE}"
            test $(stat -c%s "{ZIP_FILE}") -gt 1000000
            """
        ),
    )

    unpack = DockerOperator(
        task_id="unpack",
        image="osm-tools:latest",
        auto_remove=True,
        mounts=common_mounts,
        mount_tmp_dir=False,
        command=bash(
            f"""
            set -euo pipefail
            rm -rf "{UNZIP_DIR}"
            mkdir -p "{UNZIP_DIR}"
            unzip -q "{ZIP_FILE}" -d "{UNZIP_DIR}"

            echo "Unzipped files:"
            find "{UNZIP_DIR}" -maxdepth 3 -type f | sed -n '1,200p'

            if [ ! -f "{SRC_PARQUET_IN_ZIP}" ]; then
              echo "ERROR: expected parquet not found: {SRC_PARQUET_IN_ZIP}" >&2
              echo "Available parquet files:" >&2
              find "{UNZIP_DIR}" -type f -name "*.parquet" | sed -n '1,200p' >&2
              exit 2
            fi

            echo "OK: found source parquet:"
            ls -lh "{SRC_PARQUET_IN_ZIP}" | sed -n '1,50p'
            """
        ),
    )

    transform = DockerOperator(
        task_id="transform",
        image="osm-tools:latest",
        auto_remove=True,
        mounts=common_mounts,
        mount_tmp_dir=False,
        command=bash(
            f"""
            set -euo pipefail

            SRC=\"{SRC_PARQUET_IN_ZIP}\"

            echo "Source schema:"
            ogrinfo -ro -so \"$SRC\" \"{PARQUET_LAYER}\" | sed -n '1,120p'

            # 1) GEO output: просто копия исходного GeoParquet (EPSG:3035, WKB)
            cp -f \"$SRC\" \"{GEO_OUT}\"

            # 2) Snowflake-friendly: явные поля + geom_wkt (EPSG:4326), без бинарной геометрии
            SQL=$(cat <<'SQL'
SELECT
  {FIELDS},
  AsText(ST_Transform({GEOM_COL}, 4326)) AS geom_wkt
FROM "{PARQUET_LAYER}"
SQL
)

            # Пытаемся через ST_Transform (самый надёжный вариант)
            if ogr2ogr -f Parquet \"{SF_OUT}\" \
                  \"$SRC\" \
                  -dialect SQLITE \
                  -sql \"$SQL\" \
                  -nln \"{DATASET}_sf\" \
                  -nlt NONE \
                  -lco GEOMETRY=NONE \
                  -lco COMPRESSION=ZSTD \
                  -lco ROW_GROUP_SIZE=50000
            then
              echo "OK: created SF parquet via ST_Transform -> EPSG:4326"
            else
              echo "WARN: ST_Transform failed, fallback to -s_srs/-t_srs (less reliable in some GDAL builds)" >&2

              SQL2=$(cat <<'SQL'
SELECT
  {FIELDS},
  AsText({GEOM_COL}) AS geom_wkt
FROM "{PARQUET_LAYER}"
SQL
)

              ogr2ogr -f Parquet \"{SF_OUT}\" \
                  \"$SRC\" \
                  -s_srs EPSG:3035 -t_srs EPSG:4326 \
                  -dialect SQLITE \
                  -sql \"$SQL2\" \
                  -nln \"{DATASET}_sf\" \
                  -nlt NONE \
                  -lco GEOMETRY=NONE \
                  -lco COMPRESSION=ZSTD \
                  -lco ROW_GROUP_SIZE=50000
            fi

            echo "Outputs:"
            ls -lh \"{TRANSFORM_DIR}\" | sed -n '1,80p'

            echo "Check geom_wkt sample (должны быть градусы ~20..50, не миллионы):"
            ogr2ogr -f CSV /vsistdout/ \"{SF_OUT}\" \
              -dialect SQLITE \
              -sql "SELECT geom_wkt FROM \"{DATASET}_sf\" LIMIT 3" \
              2>/dev/null | sed -n '1,8p' || true

            # если имя слоя другое — просто выведем ogrinfo, чтобы ты увидел его в логах
            echo "SF parquet layers:"
            ogrinfo -ro -q \"{SF_OUT}\" | sed -n '1,40p'
            """
        ),
    )

    load_to_adls = PythonOperator(
        task_id="load_to_adls",
        python_callable=load_to_adls_gen2,
    )

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject=f"[SUCCESS] Eurostat population grid {DATASET} ({SCOPE})",
        html_content=f"""
        <h3>✅ Eurostat population grid ETL finished</h3>
        <ul>
          <li><b>Dataset</b>: {DATASET}</li>
          <li><b>Source Parquet layer</b>: {PARQUET_LAYER}</li>
          <li><b>Source CRS</b>: EPSG:3035</li>
          <li><b>Outputs CRS</b>: EPSG:4326</li>
          <li><b>Outputs</b>: {DATASET}.parquet (geo/WKB), {DATASET}_sf.parquet (Snowflake/geom_wkt)</li>
          <li><b>ADLS prefix</b>: {REMOTE_PREFIX}</li>
        </ul>
        """,
        conn_id="smtp_default",
    )

    failure = EmailOperator(
        task_id="failure",
        to=EMAIL_TO,
        subject=f"[FAILED] Eurostat population grid {DATASET} ({SCOPE})",
        html_content=f"""
        <h3>❌ Eurostat population grid ETL failed</h3>
        <ul>
          <li><b>Dataset</b>: {DATASET}</li>
          <li><b>Expected Parquet</b>: {SRC_PARQUET_IN_ZIP}</li>
          <li><b>ADLS prefix</b>: {REMOTE_PREFIX}</li>
        </ul>
        <p>Check Airflow task logs for details.</p>
        """,
        conn_id="smtp_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    clean_workspace >> download_zip >> check_zip >> unpack >> transform >> load_to_adls >> success
    [clean_workspace, download_zip, check_zip, unpack, transform, load_to_adls] >> failure