from datetime import timedelta
import os
import shlex
import json
import requests
from datetime import datetime, timezone

import pandas as pd

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

from docker.types import Mount
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient


# ============================================================
# CONFIG (dataset-level)
# ============================================================

DATASET = "tran_r_elvehst"
URL = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{DATASET}"

HOST_DATA_DIR = "/Volumes/T7/airflow-docker/data"
EMAIL_TO = ["coach.tutor1993@gmail.com"]

# DockerOperator works on mounted host volume at /data
BASE_DIR = f"/data/eurostat/{DATASET}"
DOWNLOAD_DIR = f"{BASE_DIR}/00_download"
TRANSFORM_DIR = f"{BASE_DIR}/02_transform"

# PythonOperator runs inside airflow containers (no /data mount by default) -> use writable /opt/airflow/data
PY_BASE_DIR = f"/opt/airflow/data/eurostat/{DATASET}"
PY_DOWNLOAD_DIR = f"{PY_BASE_DIR}/00_download"
PY_TRANSFORM_DIR = f"{PY_BASE_DIR}/02_transform"

RAW_JSON_FILE = f"{PY_DOWNLOAD_DIR}/{DATASET}.json"
TIDY_PARQUET_FILE = f"{PY_TRANSFORM_DIR}/{DATASET}_tidy.parquet"

# ---- ADLS ----
ADLS_CONTAINER = "uc-root"
ADLS_BASE_PATH = f"raw/eurostat/{DATASET}"
# snapshot-style prefix (one run = one snapshot)
# e.g. raw/eurostat/tran_r_elvehst/snapshot=2026-01-10T16-58-00Z/
# (в Snowflake удобно грузить latest по max(snapshot))
REMOTE_PREFIX = None  # будет вычислен в runtime

default_args = {
    "owner": "data-platform",
    "retries": 0,
}

common_mounts = [
    Mount(source=HOST_DATA_DIR, target="/data", type="bind")
]

def bash(cmd: str) -> str:
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
        <h3>❌ EUROSTAT PIPELINE FAILED</h3>
        <p><b>DAG:</b> {getattr(dag,'dag_id',None)}</p>
        <p><b>Run ID:</b> {getattr(dr,'run_id',None)}</p>
        <p><b>Task:</b> {getattr(ti,'task_id',None)}</p>
        <p><b>Execution date:</b> {context.get('ds')}</p>
        <p><b>Log URL:</b> <a href="{getattr(ti,'log_url',None)}">Open task logs</a></p>
        <hr/>
        <p><b>Dataset:</b> {DATASET}</p>
        <pre style="white-space:pre-wrap">{exc}</pre>
        """,
    )


# ============================================================
# 1) DOWNLOAD JSON (full dataset)
# ============================================================

def download_eurostat_json():
    os.makedirs(PY_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(PY_TRANSFORM_DIR, exist_ok=True)


    resp = requests.get(URL, timeout=1800)
    resp.raise_for_status()
    data = resp.json()

    with open(RAW_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    size_bytes = os.path.getsize(RAW_JSON_FILE)
    print(f"✔ Saved raw JSON: {RAW_JSON_FILE} ({size_bytes} bytes)")


    return {
        "dataset": DATASET,
        "raw_json": RAW_JSON_FILE,
        "raw_bytes": size_bytes,
        "id": data.get("id"),
        "size": data.get("size"),
        "value_count": len(data.get("value", {}) or {}),
    }


# ============================================================
# 2) TRANSFORM JSON -> TIDY PARQUET
# ============================================================

def decode_index(k: int, sizes: list[int]) -> list[int]:
    idxs = []
    for s in reversed(sizes):
        idxs.append(k % s)
        k //= s
    return list(reversed(idxs))

def transform_to_tidy_parquet():
    with open(RAW_JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    dim_order = data["id"]        # e.g. ["freq","vehicle","unit","geo","time"]
    sizes     = data["size"]      # e.g. [..]
    dims      = data["dimension"]

    # reverse maps index->code for each dimension
    rev = {
        dim: {v: k for k, v in dims[dim]["category"]["index"].items()}
        for dim in dim_order
    }

    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC like Snowflake TIMESTAMP_NTZ

    rows = []
    values = data.get("value", {}) or {}

    for k_str, val in values.items():
        k = int(k_str)
        idxs = decode_index(k, sizes)

        row_dims = {dim: rev[dim][idx] for dim, idx in zip(dim_order, idxs)}
        row_dims["dataset"] = DATASET
        row_dims["value"] = float(val) if val is not None else None
        row_dims["ingest_ts"] = now_ts
        rows.append(row_dims)

    if not rows:
        raise RuntimeError("No values parsed from Eurostat JSON (empty dataset?)")

    df = pd.DataFrame(rows)

    # Ensure stable column order: dataset, dims..., value, ingest_ts
    ordered_cols = ["dataset"] + dim_order + ["value", "ingest_ts"]
    df = df[ordered_cols]

    os.makedirs(PY_TRANSFORM_DIR, exist_ok=True)
    df.to_parquet(TIDY_PARQUET_FILE, index=False)

    size_bytes = os.path.getsize(TIDY_PARQUET_FILE)
    print(f"✔ Saved tidy parquet: {TIDY_PARQUET_FILE} ({size_bytes} bytes)")
    print(f"✔ Rows: {len(df)} | Cols: {len(df.columns)}")
    print(df.head(3).to_string(index=False))

    return {
        "dataset": DATASET,
        "tidy_parquet": TIDY_PARQUET_FILE,
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "parquet_bytes": size_bytes,
        "columns": list(df.columns),
    }


# ============================================================
# 3) UPLOAD TO ADLS (raw json + parquet)
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

    if not os.path.isfile(RAW_JSON_FILE):
        raise RuntimeError(f"Expected raw JSON not found: {RAW_JSON_FILE}")
    if not os.path.isfile(TIDY_PARQUET_FILE):
        raise RuntimeError(f"Expected parquet not found: {TIDY_PARQUET_FILE}")

    # snapshot prefix
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    remote_prefix = f"{ADLS_BASE_PATH}/snapshot={ts}/"

    uploads = []
    for local_path in [RAW_JSON_FILE, TIDY_PARQUET_FILE]:
        blob_name = remote_prefix + os.path.basename(local_path)
        size_bytes = os.path.getsize(local_path)
        print(f"Uploading: {local_path} -> {blob_name} ({size_bytes} bytes)")

        with open(local_path, "rb") as fh:
            container.upload_blob(blob_name, fh, overwrite=True)

        uploads.append({"name": os.path.basename(local_path), "bytes": size_bytes, "blob": blob_name})

    return {
        "dataset": DATASET,
        "container": ADLS_CONTAINER,
        "remote_prefix": remote_prefix,
        "uploaded_files": len(uploads),
        "files": uploads,
    }


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=f"eurostat_{DATASET}_full_to_adls",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_failure,
    tags=["eurostat", "json", "parquet", "adls", "bronze"],
) as dag:

    # clean workspace (same pattern as you use)
    clean_workspace = DockerOperator(
        task_id="clean_workspace",
        image="osm-tools:latest",
        auto_remove=True,
        command=bash(f"""
            rm -rf "{DOWNLOAD_DIR}" "{TRANSFORM_DIR}"
            mkdir -p "{DOWNLOAD_DIR}" "{TRANSFORM_DIR}"
            ls -la "{BASE_DIR}" || true
        """),
        mounts=common_mounts,
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )

    download_json = PythonOperator(
        task_id="download_json",
        python_callable=download_eurostat_json,
    )

    transform_parquet = PythonOperator(
        task_id="transform_to_tidy_parquet",
        python_callable=transform_to_tidy_parquet,
    )


    def validate_outputs_py():
        if not os.path.isfile(RAW_JSON_FILE) or os.path.getsize(RAW_JSON_FILE) == 0:
            raise RuntimeError(f"Raw JSON missing/empty: {RAW_JSON_FILE}")
        if not os.path.isfile(TIDY_PARQUET_FILE) or os.path.getsize(TIDY_PARQUET_FILE) == 0:
            raise RuntimeError(f"Tidy parquet missing/empty: {TIDY_PARQUET_FILE}")

        print("Raw JSON size:", os.path.getsize(RAW_JSON_FILE), "bytes")
        print("Parquet size:", os.path.getsize(TIDY_PARQUET_FILE), "bytes")

        # Parquet sanity: rowcount + sample
        df = pd.read_parquet(TIDY_PARQUET_FILE)
        print("ROWCOUNT:", len(df))
        if len(df) == 0:
            raise RuntimeError("Parquet has 0 rows")

        # Require expected core columns
        required = {"dataset", "value", "ingest_ts"}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"Missing required columns in parquet: {sorted(missing)}")

        # Extra: show schema and sample
        print("Columns:", list(df.columns))
        print(df.head(3).to_string(index=False))

        return {
            "raw_json_bytes": int(os.path.getsize(RAW_JSON_FILE)),
            "parquet_bytes": int(os.path.getsize(TIDY_PARQUET_FILE)),
            "rows": int(len(df)),
            "cols": int(df.shape[1]),
            "columns": list(df.columns),
        }

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_outputs_py,
    )

    load_to_adls = PythonOperator(
        task_id="load_to_adls",
        python_callable=load_to_adls_gen2,
    )

    success_email = EmailOperator(
        task_id="success_email",
        to=EMAIL_TO,
        subject=f"[AIRFLOW SUCCESS] eurostat_{DATASET} | full snapshot -> ADLS",
        html_content=f"""
        <h3>✅ EUROSTAT {DATASET} READY (raw JSON + tidy parquet in ADLS)</h3>
        <p><b>DAG:</b> {{{{ dag.dag_id }}}}</p>
        <p><b>Run ID:</b> {{{{ dag_run.run_id }}}}</p>
        <p><b>Execution date (ds):</b> {{{{ ds }}}}</p>
        <hr/>
        <p><b>Local JSON:</b> <code>{RAW_JSON_FILE}</code></p>
        <p><b>Local Parquet:</b> <code>{TIDY_PARQUET_FILE}</code></p>
        <hr/>
        <p><b>ADLS container:</b> <code>{ADLS_CONTAINER}</code></p>
        <p><b>ADLS prefix:</b> <code>{{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('remote_prefix') }}}}</code></p>
        <p><b>Uploaded files:</b> {{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('uploaded_files') }}}}</p>
        <pre style="white-space:pre-wrap">{{{{ (ti.xcom_pull(task_ids='load_to_adls') or dict()).get('files') }}}}</pre>
        <hr/>
        <p><b>Next step (Snowflake):</b> external stage -> parquet -> MERGE/overwrite bronze table.</p>
        """,
    )

    clean_workspace >> download_json >> transform_parquet >> validate_outputs >> load_to_adls >> success_email