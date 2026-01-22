from datetime import timedelta
import os
import shlex

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

from docker.types import Mount
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import base64
from azure.core.pipeline.transport import RequestsTransport
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# Workaround for Azure SDK passing 'hosts' kwarg into requests (can happen with some azure-core/requests combos)
class PatchedRequestsTransport(RequestsTransport):
    def send(self, request, **kwargs):
        kwargs.pop("hosts", None)
        return super().send(request, **kwargs)

# ============================================================
# CONFIG
# ============================================================

DATASET = "degurba_lau_2021"
YEAR = "2021"

BASE_DIR = f"/data/eurostat/{DATASET}"
DOWNLOAD_DIR = f"{BASE_DIR}/00_download"
EXTRACT_DIR  = f"{BASE_DIR}/01_extract"
TRANSFORM_DIR = f"{BASE_DIR}/02_transform"

LOCAL_TRANSFORM_DIR = f"/opt/airflow/data/eurostat/{DATASET}/02_transform"

PARQUET_FILE = f"{TRANSFORM_DIR}/lau_degurba_2021_sf.parquet"
PARQUET_BASENAME = os.path.basename(PARQUET_FILE)
LOCAL_PARQUET_FILE = f"{LOCAL_TRANSFORM_DIR}/{PARQUET_BASENAME}"

ZIP_FILE = f"{DOWNLOAD_DIR}/DGURBA_RG_01M_2021_4258.zip"
SHP_FILE = f"{EXTRACT_DIR}/DGURBA_RG_01M_2021_4258.shp"


URL = (
    "https://ec.europa.eu/eurostat/documents/7116161/15125332/"
    "DGURBA_RG_01M_2021_4258.zip/"
    "09a07de6-f16e-af72-1178-123259fadefd"
)

HOST_DATA_DIR = "/Volumes/T7/airflow-docker/data"
EMAIL_TO = ["coach.tutor1993@gmail.com"]

# ---------------- ADLS ----------------
ADLS_CONTAINER = "uc-root"
ADLS_BASE_PATH = "raw/eurostat/degurba"
REMOTE_PREFIX = f"{ADLS_BASE_PATH}/lau/year={YEAR}/"

# Upload tuning (helps with slow connections / large files)
UPLOAD_TIMEOUT_SEC = int(os.getenv("AZURE_UPLOAD_TIMEOUT_SEC", "1800"))  # 30 min
UPLOAD_READ_TIMEOUT_SEC = int(os.getenv("AZURE_UPLOAD_READ_TIMEOUT_SEC", "1800"))
UPLOAD_CONN_TIMEOUT_SEC = int(os.getenv("AZURE_UPLOAD_CONN_TIMEOUT_SEC", "60"))


default_args = {
    "owner": "data-platform",
    "retries": 0,
}

# ============================================================
# MOUNTS
# ============================================================

common_mounts = [
    Mount(source=HOST_DATA_DIR, target="/data", type="bind")
]

def bash(cmd: str) -> str:
    """Return a safe /bin/bash -lc command without manual quote escaping.

    We shell-quote the whole script via shlex.quote(), so inside `cmd` you can use
    normal double-quotes without writing \\\" everywhere.
    """
    return f"/bin/bash -lc {shlex.quote(cmd.strip())}"

# ============================================================
# FAILURE EMAIL
# ============================================================

def notify_failure(context):
    ti = context.get("task_instance")
    exc = context.get("exception")
    dag = context.get("dag")
    dr = context.get("dag_run")

    send_email(
        to=EMAIL_TO,
        subject=f"[AIRFLOW FAILED] eurostat_{DATASET} | task={getattr(ti,'task_id',None)} | run={getattr(dr,'run_id',None)}",
        html_content=f"""
        <h3>❌ DGURBA PIPELINE FAILED</h3>
        <p><b>DAG:</b> {getattr(dag,'dag_id',None)}</p>
        <p><b>Run ID:</b> {getattr(dr,'run_id',None)}</p>
        <p><b>Task:</b> {getattr(ti,'task_id',None)}</p>
        <p><b>Try:</b> {getattr(ti,'try_number',None)} / {getattr(ti,'max_tries',None)}</p>
        <p><b>Execution date:</b> {context.get('ds')}</p>
        <p><b>Log URL:</b> <a href="{getattr(ti,'log_url',None)}">Open task logs</a></p>
        <hr/>
        <p><b>Dataset:</b> {DATASET}</p>
        <p><b>Year:</b> {YEAR}</p>
        <p><b>ADLS prefix:</b> {REMOTE_PREFIX}</p>
        <p><b>Local parquet:</b> {LOCAL_TRANSFORM_DIR}</p>
        <hr/>
        <pre style="white-space:pre-wrap">{exc}</pre>
        """,
    )

# ============================================================
# LOAD TO ADLS GEN2 (SAME AS OSM)
# ============================================================

def load_to_adls_gen2():

    storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]
    account_url = f"https://{storage_account}.blob.core.windows.net"

    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    service = BlobServiceClient(account_url, credential=credential)
    container = service.get_container_client(ADLS_CONTAINER)

    if not os.path.isfile(LOCAL_PARQUET_FILE):
        raise RuntimeError(f"Expected parquet not found: {LOCAL_PARQUET_FILE}")

    blob_name = REMOTE_PREFIX + os.path.basename(LOCAL_PARQUET_FILE)
    size_bytes = os.path.getsize(LOCAL_PARQUET_FILE)
    print(f"Uploading to ADLS: {blob_name} ({size_bytes} bytes)")


    with open(LOCAL_PARQUET_FILE, "rb") as fh:
        container.upload_blob(blob_name, fh, overwrite=True)

    return {
        "dataset": DATASET,
        "year": YEAR,
        "container": ADLS_CONTAINER,
        "remote_prefix": REMOTE_PREFIX,
        "uploaded_files": 1,
        "files": [{"name": os.path.basename(LOCAL_PARQUET_FILE), "bytes": size_bytes, "blob": blob_name}],
    }

def upload_block_blob_in_chunks(blob_client, file_path: str, chunk_size: int = 8 * 1024 * 1024) -> int:
    """Upload file as a block blob in chunks to avoid a single large PUT request.

    Returns number of bytes uploaded.
    """
    block_ids = []
    uploaded_bytes = 0
    idx = 0

    with open(file_path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break

            # Block IDs must be base64-encoded strings
            block_id = base64.b64encode(f"{idx:06d}".encode("utf-8")).decode("utf-8")
            blob_client.stage_block(block_id=block_id, data=chunk, timeout=UPLOAD_TIMEOUT_SEC)
            block_ids.append(block_id)

            uploaded_bytes += len(chunk)
            idx += 1

    blob_client.commit_block_list(block_ids, timeout=UPLOAD_TIMEOUT_SEC)
    return uploaded_bytes

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=f"eurostat_{DATASET}_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_failure,
    tags=["eurostat", "degurba", "lau", "spatial", "bronze"],
) as dag:

    # ------------------
    # CLEAN WORKSPACE
    # ------------------
    clean_workspace = DockerOperator(
        task_id="clean_workspace",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            rm -rf "{DOWNLOAD_DIR}" "{EXTRACT_DIR}" "{TRANSFORM_DIR}"
            mkdir -p "{DOWNLOAD_DIR}" "{EXTRACT_DIR}" "{TRANSFORM_DIR}"
            ls -la "{BASE_DIR}" || true
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # DOWNLOAD ZIP
    # ------------------
    download_zip = DockerOperator(
        task_id="download_zip",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            curl -fL --retry 5 --retry-delay 3 \
              "{URL}" \
              -o "{ZIP_FILE}"
            ls -lh "{ZIP_FILE}"
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # CHECK TOOLS
    # ------------------
    check_tools = DockerOperator(
        task_id="check_tools",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(r"""
            set -euo pipefail
            ogr2ogr --version
            ogr2ogr --formats | grep -i parquet
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # UNZIP
    # ------------------
    unzip = DockerOperator(
        task_id="unzip",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            unzip -o "{ZIP_FILE}" -d "{EXTRACT_DIR}"
            shp=$(find "{EXTRACT_DIR}" -maxdepth 3 -type f -name '*.shp' | head -n 1)
            if [ -z "$shp" ]; then
              echo 'ERROR: .shp not found after unzip'
              find "{EXTRACT_DIR}" -maxdepth 3 -type f | head -n 50
              exit 1
            fi
            echo "$shp" > "{EXTRACT_DIR}/_shp_path.txt"
            ls -lh "$shp"
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # SHP → GEOPARQUET
    # ------------------
    transform_to_parquet = DockerOperator(
        task_id="transform_geoparquet",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            set -euo pipefail

            shp=$(cat "{EXTRACT_DIR}/_shp_path.txt")
            echo "Using SHP: $shp"

            # Detect layer name without awk (avoid braces in Python f-strings)
            layer=$(ogrinfo -ro -so "$shp" \
              | sed -nE 's/^[0-9]+:[[:space:]]*//p' \
              | head -n 1 \
              | sed -E 's/[[:space:]]*\(.*$//')
            if [ -z "$layer" ]; then
              echo "ERROR: could not detect layer name from $shp"
              ogrinfo -ro -so "$shp" || true
              exit 1
            fi
            echo "Detected layer: $layer"

            # For this dataset we KNOW the attribute columns. Avoid parsing ogrinfo output (brittle across GDAL builds).
            fields='"GISCO_ID","CNTR_CODE","LAU_ID","LAU_NAME","DGURBA"'

            sql="SELECT $fields, AsText(geometry) AS geom_wkt FROM \"$layer\""
            echo "Using SQL: $sql"

            # NOTE:
            # - source CRS: EPSG:4258 (ETRS89)
            # - target CRS: EPSG:4326 (WGS84)
            # - GEOMETRY=NONE => no binary geometry column in Parquet; only geom_wkt text
            ogr2ogr -makevalid -skipfailures \
              -f Parquet "{PARQUET_FILE}" \
              "$shp" \
              -s_srs EPSG:4258 \
              -t_srs EPSG:4326 \
              -dialect SQLITE \
              -sql "$sql" \
              -nlt NONE \
              -lco GEOMETRY=NONE \
              -lco FID=FID \
              -lco WRITE_BBOX=YES \
              -lco COMPRESSION=ZSTD \
              -lco ROW_GROUP_SIZE=50000

            ls -lh "{PARQUET_FILE}"
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # DATA SLA
    # ------------------
    validate_parquet = DockerOperator(
        task_id="validate_parquet",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            ogrinfo "{PARQUET_FILE}" -so
            # Extract feature count without awk (avoid braces in Python f-strings)
            count=$(ogrinfo "{PARQUET_FILE}" -ro -al \
              | grep -m1 "Feature Count" \
              | sed -nE 's/.*Feature Count:[[:space:]]*([0-9]+).*/\1/p')
            echo "Feature count: $count"
            test "$count" -gt 0

            echo "Checking that geom_wkt column exists..."
            ogrinfo "{PARQUET_FILE}" -so | grep -i "geom_wkt" || (echo "ERROR: geom_wkt column not found"; exit 1)

            echo "Checking geometry in parquet (expect Geometry: None due to GEOMETRY=NONE)..."
            geom_line=$(ogrinfo "{PARQUET_FILE}" -so | grep -i "^Geometry:" || true)
            if [ -z "$geom_line" ]; then
              echo "Geometry line: <missing>"
            else
              echo "Geometry line: $geom_line"
            fi
            if [ -n "$geom_line" ] && ! echo "$geom_line" | grep -Eiq "Geometry:[[:space:]]*None"; then
              echo "ERROR: Parquet still reports a geometry type (expected None). Snowflake may choke on binary data."
              ogrinfo "{PARQUET_FILE}" -so
              exit 1
            fi

            echo "Sample row (geom_wkt should be visible below):"
            ogrinfo "{PARQUET_FILE}" -ro -al -geom=NO -limit 1 || true

            echo "dataset={DATASET}" > "{TRANSFORM_DIR}/_stats.txt"
            echo "year={YEAR}" >> "{TRANSFORM_DIR}/_stats.txt"
            echo "parquet={PARQUET_FILE}" >> "{TRANSFORM_DIR}/_stats.txt"
            echo "feature_count=$count" >> "{TRANSFORM_DIR}/_stats.txt"
            ls -lh "{PARQUET_FILE}" >> "{TRANSFORM_DIR}/_stats.txt"
            echo "---" >> "{TRANSFORM_DIR}/_stats.txt"
            ogrinfo "{PARQUET_FILE}" -so >> "{TRANSFORM_DIR}/_stats.txt" || true
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # LOAD TO ADLS
    # ------------------
    load_to_adls = PythonOperator(
        task_id="load_to_adls",
        python_callable=load_to_adls_gen2,
    )


    # ------------------
    # SUCCESS MAIL
    # ------------------
    success_email = EmailOperator(
        task_id="success_email",
        to=EMAIL_TO,
        subject=f"[AIRFLOW SUCCESS] eurostat_{DATASET} | year={YEAR} | ADLS",
        html_content=f"""
        <h3>✅ DGURBA LAU {YEAR} READY (ADLS, _sf.parquet with geom_wkt)</h3>
        <p><b>DAG:</b> {{{{ dag.dag_id }}}}</p>
        <p><b>Run ID:</b> {{{{ dag_run.run_id }}}}</p>
        <p><b>Execution date (ds):</b> {{{{ ds }}}}</p>
        <p><b>Logical date:</b> {{{{ logical_date }}}}</p>
        <hr/>
        <p><b>Dataset:</b> {DATASET}</p>
        <p><b>Local parquet (Airflow path):</b> <code>{LOCAL_PARQUET_FILE}</code></p>
        <p><b>Local stats:</b> <code>{TRANSFORM_DIR}/_stats.txt</code></p>
        <p><b>ADLS container:</b> <code>{ADLS_CONTAINER}</code></p>
        <p><b>ADLS prefix:</b> <code>{REMOTE_PREFIX}</code></p>
        <hr/>
        <h4>Upload summary (XCom from load_to_adls)</h4>
        <p><b>Uploaded files:</b> {{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('uploaded_files') }}}}</p>
        <pre style="white-space:pre-wrap">{{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('files') }}}}</pre>
        <hr/>
        <p><b>Next step (Snowflake):</b> run ELT inside Snowflake (stage/copy/dynamic tables). Airflow currently owns only the ADLS delivery.</p>
        """,
    )

    # ================= DEPENDENCIES =================
    clean_workspace >> download_zip >> check_tools >> unzip
    unzip >> transform_to_parquet >> validate_parquet
    validate_parquet >> load_to_adls >> success_email