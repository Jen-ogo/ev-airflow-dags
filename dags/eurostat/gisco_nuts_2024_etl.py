from datetime import timedelta
import os
import shlex
import glob
from typing import Dict, List

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

from docker.types import Mount
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport

# Workaround for Azure SDK passing 'hosts' kwarg into requests (can happen with some azure-core/requests combos)
class PatchedRequestsTransport(RequestsTransport):
    def send(self, request, **kwargs):
        kwargs.pop("hosts", None)
        return super().send(request, **kwargs)

# ============================================================
# CONFIG
# ============================================================

DATASET = "gisco_nuts"
YEAR = "2024"
SCALE = "01m"
CRS = "4326"
LEVELS = [0, 1, 2, 3]  

BASE_DIR = f"/data/gisco/{DATASET}/year={YEAR}/scale={SCALE}/crs={CRS}"
DOWNLOAD_DIR = f"{BASE_DIR}/00_download"
EXTRACT_DIR  = f"{BASE_DIR}/01_extract"
TRANSFORM_DIR = f"{BASE_DIR}/02_transform"

LOCAL_TRANSFORM_DIR = f"/opt/airflow/data/gisco/{DATASET}/year={YEAR}/scale={SCALE}/crs={CRS}/02_transform"

# Where resulting parquets will appear (one per level)
# We intentionally keep filenames explicit, and partitioning goes into ADLS path.
PARQUET_PATTERN = f"{TRANSFORM_DIR}/nuts_rg_{SCALE}_{YEAR}_{CRS}_level=*.parquet"

# GISCO API
GISCO_BASE = "https://gisco-services.ec.europa.eu/distribution/v2/nuts"
DATASETS_JSON_URL = f"{GISCO_BASE}/datasets.json"

HOST_DATA_DIR = "/Volumes/T7/airflow-docker/data"
EMAIL_TO = ["coach.tutor1993@gmail.com"]

# ---------------- ADLS ----------------
ADLS_CONTAINER = "uc-root"
ADLS_BASE_PATH = "raw/gisco/nuts"
REMOTE_PREFIX = f"{ADLS_BASE_PATH}/year={YEAR}/scale={SCALE}/crs={CRS}/"  # дальше level=...

# Upload tuning
UPLOAD_TIMEOUT_SEC = int(os.getenv("AZURE_UPLOAD_TIMEOUT_SEC", "1800"))
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
    """Return a safe /bin/bash -lc command without manual quote escaping."""
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
        subject=f"[AIRFLOW FAILED] {DATASET} | year={YEAR} | task={getattr(ti,'task_id',None)} | run={getattr(dr,'run_id',None)}",
        html_content=f"""
        <h3>❌ GISCO NUTS PIPELINE FAILED</h3>
        <p><b>DAG:</b> {getattr(dag,'dag_id',None)}</p>
        <p><b>Run ID:</b> {getattr(dr,'run_id',None)}</p>
        <p><b>Task:</b> {getattr(ti,'task_id',None)}</p>
        <p><b>Try:</b> {getattr(ti,'try_number',None)} / {getattr(ti,'max_tries',None)}</p>
        <p><b>Execution date:</b> {context.get('ds')}</p>
        <p><b>Log URL:</b> <a href="{getattr(ti,'log_url',None)}">Open task logs</a></p>
        <hr/>
        <p><b>Dataset:</b> {DATASET}</p>
        <p><b>Year:</b> {YEAR}</p>
        <p><b>Scale:</b> {SCALE}</p>
        <p><b>CRS:</b> EPSG:{CRS}</p>
        <p><b>Levels:</b> {LEVELS}</p>
        <p><b>ADLS prefix:</b> {REMOTE_PREFIX}</p>
        <p><b>Local transform:</b> {LOCAL_TRANSFORM_DIR}</p>
        <hr/>
        <pre style="white-space:pre-wrap">{exc}</pre>
        """,
    )

# ============================================================
# LOAD TO ADLS GEN2 (MULTI-FILE)
# ============================================================

def load_to_adls_gen2() -> Dict:
    storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]
    account_url = f"https://{storage_account}.blob.core.windows.net"

    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    # transport patch (safe)
    transport = PatchedRequestsTransport(connection_timeout=UPLOAD_CONN_TIMEOUT_SEC, read_timeout=UPLOAD_READ_TIMEOUT_SEC)

    service = BlobServiceClient(account_url, credential=credential, transport=transport)
    container = service.get_container_client(ADLS_CONTAINER)

    files = sorted(glob.glob(os.path.join(LOCAL_TRANSFORM_DIR, "nuts_rg_*.parquet")))
    if not files:
        raise RuntimeError(f"No parquet files found in: {LOCAL_TRANSFORM_DIR}")

    uploaded = []
    for fp in files:
        base = os.path.basename(fp)

        # derive level from filename "level=X"
        level_part = "level=unknown"
        if "level=" in base:
            level_part = base.split("level=", 1)[1].split(".", 1)[0]
            level_part = f"level={level_part}"

        blob_name = f"{REMOTE_PREFIX}{level_part}/{base}"
        size_bytes = os.path.getsize(fp)
        print(f"Uploading to ADLS: {blob_name} ({size_bytes} bytes)")

        with open(fp, "rb") as fh:
            container.upload_blob(blob_name, fh, overwrite=True)

        uploaded.append({"name": base, "bytes": size_bytes, "blob": blob_name})

    return {
        "dataset": DATASET,
        "year": YEAR,
        "scale": SCALE,
        "crs": CRS,
        "levels": LEVELS,
        "container": ADLS_CONTAINER,
        "remote_prefix": REMOTE_PREFIX,
        "uploaded_files": len(uploaded),
        "files": uploaded,
    }

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=f"{DATASET}_{YEAR}_{SCALE}_{CRS}_etl",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_failure,
    tags=["gisco", "nuts", "spatial", "bronze"],
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
            python3 --version
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # DOWNLOAD GEOJSONs FROM GISCO API (datasets.json -> files.json -> urls)
    # ------------------
    download_geojson = DockerOperator(
        task_id="download_geojson",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            set -euo pipefail
            echo "VALIDATE_PARQUET_SCRIPT=v2 (layer-aware schema check)"
            cd "{DOWNLOAD_DIR}"

            echo "Downloading GISCO NUTS GeoJSON (direct URL pattern; year={YEAR}, scale={SCALE}, crs={CRS})..."
            echo "Base: {GISCO_BASE}/geojson/"

            for lvl in {" ".join(str(x) for x in LEVELS)}; do
              name="NUTS_RG_{SCALE.upper()}_{YEAR}_{CRS}_LEVL_${{lvl}}.geojson"
              url="{GISCO_BASE}/geojson/${{name}}"

              echo "  -> level=${{lvl}} :: $url"

              # Try plain .geojson first
              if curl -fsSL --retry 5 --retry-delay 3 "$url" -o "$name"; then
                :
              else
                # Fallback: some GISCO releases ship .geojson.gz
                echo "     plain geojson not found, trying gzip..."
                gz_url="${{url}}.gz"
                gz_name="${{name}}.gz"
                curl -fsSL --retry 5 --retry-delay 3 "$gz_url" -o "$gz_name"
                gunzip -f "$gz_name"
              fi

              ls -lh "$name"
            done

            echo "Downloaded files:"
            ls -lh *.geojson || true
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # TRANSFORM: GeoJSON -> Parquet with geom_wkt (TEXT), no binary geometry
    # ------------------
    transform_to_parquet = DockerOperator(
        task_id="transform_to_parquet",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            set -euo pipefail

            cd "{DOWNLOAD_DIR}"

            for lvl in {" ".join(str(x) for x in LEVELS)}; do
              in="NUTS_RG_{SCALE.upper()}_{YEAR}_{CRS}_LEVL_${{lvl}}.geojson"
              test -f "$in"

              out="{TRANSFORM_DIR}/nuts_rg_{SCALE}_{YEAR}_{CRS}_level=${{lvl}}.parquet"
              echo "---- level=${{lvl}} ----"
              echo "IN : $in"
              echo "OUT: $out"

              # IMPORTANT:
              # - GeoJSON layer name is not always "OGRGeoJSON". Detect it via ogrinfo.
              # - GEOMETRY=NONE => no binary geometry, only geom_wkt text
              layer=$(ogrinfo -ro -so "$in" \
                | sed -nE 's/^[0-9]+:[[:space:]]*//p' \
                | head -n 1 \
                | sed -E 's/[[:space:]]*\(.*$//')
              if [ -z "$layer" ]; then
                echo "ERROR: could not detect layer name from $in"
                ogrinfo -ro -so "$in" || true
                exit 1
              fi
              echo "Detected layer: $layer"
              echo "ogrinfo (first lines):"
              ogrinfo -ro -so "$in" | head -n 30 || true

              sql=$(printf 'SELECT NUTS_ID, CNTR_CODE, NAME_LATN, LEVL_CODE, AsText(geometry) AS geom_wkt FROM "%s"' "$layer")
              echo "Using SQL: $sql"

              ogr2ogr -makevalid -skipfailures \
                -f Parquet "$out" "$in" \
                -dialect SQLITE \
                -sql "$sql" \
                -nlt NONE \
                -lco GEOMETRY=NONE \
                -lco COMPRESSION=ZSTD \
                -lco ROW_GROUP_SIZE=50000

              ls -lh "$out"
            done
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    # ------------------
    # VALIDATE: rowcount + geom_wkt exists + sample WKT
    # ------------------
    validate_parquet = DockerOperator(
        task_id="validate_parquet",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            set -euo pipefail

            mkdir -p "{TRANSFORM_DIR}"
            stats="{TRANSFORM_DIR}/_stats.txt"
            : > "$stats"

            echo "dataset={DATASET}" >> "$stats"
            echo "year={YEAR}" >> "$stats"
            echo "scale={SCALE}" >> "$stats"
            echo "crs=EPSG:{CRS}" >> "$stats"
            echo "levels={LEVELS}" >> "$stats"
            echo "---" >> "$stats"

            for lvl in {" ".join(str(x) for x in LEVELS)}; do
              f="{TRANSFORM_DIR}/nuts_rg_{SCALE}_{YEAR}_{CRS}_level=${{lvl}}.parquet"
              test -f "$f"

              echo "========================" | tee -a "$stats"
              echo "LEVEL=${{lvl}}" | tee -a "$stats"
              echo "FILE=$f" | tee -a "$stats"

              # rowcount
              # NOTE: `ogrinfo -so` for Parquet does NOT include Feature Count.
              # Also, with `set -o pipefail`, pipelines can be brittle (SIGPIPE=141).
              # So we dump ogrinfo output to a temp file and parse it without pipes.
              tmp=$(mktemp)
              ogrinfo "$f" -ro -al > "$tmp" 2>&1 || true

              count=$(python3 - "$tmp" <<'PY'
import re, sys
p = sys.argv[1]
with open(p, 'r', errors='ignore') as f:
    for line in f:
        m = re.search(r'Feature Count:\s*([0-9]+)', line)
        if m:
            print(m.group(1))
            break
PY
)
              
              if [ -z "$count" ]; then
                echo "ERROR: could not extract Feature Count" | tee -a "$stats"
                echo "--- ogrinfo output (first 120 lines) ---" | tee -a "$stats"
                sed -n '1,120p' "$tmp" | tee -a "$stats" || true
                rm -f "$tmp"
                exit 1
              fi
              rm -f "$tmp"

              echo "Feature count: $count" | tee -a "$stats"
              if ! echo "$count" | grep -Eq '^[0-9]+$'; then
                echo "ERROR: Feature Count is not numeric: $count" | tee -a "$stats"
                exit 1
              fi
              test "$count" -gt 0

              # Parquet driver часто не показывает поля, если не указать layer.
              # Поэтому детектим layer и используем его для -so/-al.
              layer=$(ogrinfo -ro -so "$f" \
                | sed -nE 's/^[0-9]+:[[:space:]]*//p' \
                | head -n 1 \
                | sed -E 's/[[:space:]]*\(.*$//')
              if [ -z "$layer" ]; then
                echo "ERROR: could not detect Parquet layer name for level=${{lvl}}" | tee -a "$stats"
                ogrinfo -ro -so "$f" | tee -a "$stats" || true
                exit 1
              fi
              echo "Detected layer: $layer" | tee -a "$stats"

              so_tmp=$(mktemp)
              ogrinfo -ro -so "$f" "$layer" > "$so_tmp" 2>&1 || true

              # ensure geom_wkt exists
              echo "Schema check (geom_wkt must exist)..." | tee -a "$stats"
              echo "(debug) ogrinfo -so was: ogrinfo -ro -so \"$f\" \"$layer\"" | tee -a "$stats"
              if ! grep -qi "geom_wkt" "$so_tmp"; then
                echo "ERROR: geom_wkt column not found for level=${{lvl}}" | tee -a "$stats"
                echo "--- ogrinfo -so output (first 200 lines) ---" | tee -a "$stats"
                sed -n '1,200p' "$so_tmp" | tee -a "$stats" || true
                rm -f "$so_tmp"
                exit 1
              fi
              grep -i "geom_wkt" "$so_tmp" | tee -a "$stats" || true

              # ensure parquet has no geometry
              geom_line=$(grep -i "^Geometry:" "$so_tmp" || true)
              if [ -z "$geom_line" ]; then
                echo "Geometry line: <missing>" | tee -a "$stats"
              else
                echo "Geometry line: $geom_line" | tee -a "$stats"
              fi
              if [ -n "$geom_line" ] && ! echo "$geom_line" | grep -Eiq "Geometry:[[:space:]]*None"; then
                echo "ERROR: Parquet reports non-None geometry (expected None)." | tee -a "$stats"
                sed -n '1,200p' "$so_tmp" | tee -a "$stats" || true
                rm -f "$so_tmp"
                exit 1
              fi

              rm -f "$so_tmp"

              # sample row + sample geom_wkt (WKT обязательно)
              echo "Sample row (expect geom_wkt WKT text):" | tee -a "$stats"
              ogrinfo -ro -al -geom=NO -limit 1 "$f" "$layer" | tee -a "$stats" || true

              echo "" >> "$stats"
            done

            echo "Wrote stats: $stats"
            tail -n 80 "$stats" || true
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
        subject=f"[AIRFLOW SUCCESS] {DATASET} | year={YEAR} | scale={SCALE} | crs={CRS} | ADLS",
        html_content=f"""
        <h3>✅ GISCO NUTS {YEAR} READY (ADLS, Parquet with geom_wkt)</h3>
        <p><b>DAG:</b> {{{{ dag.dag_id }}}}</p>
        <p><b>Run ID:</b> {{{{ dag_run.run_id }}}}</p>
        <p><b>Execution date (ds):</b> {{{{ ds }}}}</p>
        <p><b>Logical date:</b> {{{{ logical_date }}}}</p>
        <hr/>
        <p><b>Dataset:</b> {DATASET}</p>
        <p><b>Year:</b> {YEAR}</p>
        <p><b>Scale:</b> {SCALE}</p>
        <p><b>CRS:</b> EPSG:{CRS}</p>
        <p><b>Levels:</b> {LEVELS}</p>
        <hr/>
        <p><b>Local stats:</b> <code>{TRANSFORM_DIR}/_stats.txt</code></p>
        <p><b>ADLS container:</b> <code>{ADLS_CONTAINER}</code></p>
        <p><b>ADLS prefix:</b> <code>{REMOTE_PREFIX}</code></p>
        <hr/>
        <h4>Upload summary (XCom from load_to_adls)</h4>
        <p><b>Uploaded files:</b> {{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('uploaded_files') }}}}</p>
        <pre style="white-space:pre-wrap">{{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('files') }}}}</pre>
        <hr/>
        <p><b>Next step (Snowflake):</b> create BRONZE external stage/copy + SILVER dynamic tables (TRY_TO_GEOGRAPHY(geom_wkt)).</p>
        """,
    )

    # ================= DEPENDENCIES =================
    clean_workspace >> check_tools >> download_geojson >> transform_to_parquet >> validate_parquet >> load_to_adls >> success_email