from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from docker.types import Mount

from ev_repository.dags.osm.osm_config import (
    EMAIL_TO,
    HOST_DATA_DIR,
    ADLS_CONTAINER,
    ADLS_BASE_PATH,
    DATASET_EXTRACT,
    SNOWFLAKE_COLUMNS,
    DATASET_SOURCE,
    REGION_PBF_URL_TMPL,
    COUNTRY_PBF_URL_TMPL,
    REGION_POLY_URL_TMPL,
    OSMIUM_ADMIN_EXTRACT,
)


def bash(cmd: str):
    """Bash wrapper for DockerOperator.

    IMPORTANT: return argv-list to avoid fragile shell quoting/escaping.
    """
    return ["/bin/bash", "-lc", cmd.strip()]


def make_paths(country: str, region: str, dataset: str, ds: str = "{{ ds }}"):
    base = f"/data/{country}/{region}/{dataset}"
    download_dir = f"/data/{country}/{region}/_download"
    extract_dir = f"{base}/01_extract"
    transform_dir = f"{base}/02_transform"

    # inputs
    region_pbf_file = f"{download_dir}/{region}-latest.osm.pbf"
    country_pbf_file = f"{download_dir}/{country}-latest.osm.pbf"
    region_poly_file = f"{download_dir}/{region}.poly"

    # osmium cut output (only for admin when routed)
    cut_dir = f"/data/{country}/{region}/{OSMIUM_ADMIN_EXTRACT['cut_dirname']}/dt={ds}"
    cut_pbf_file = f"{cut_dir}/{OSMIUM_ADMIN_EXTRACT['cut_prefix']}{region}.osm.pbf"

    # airflow-container local path to parquet (must exist via your docker-compose volume mapping)
    local_transform_dir = f"/opt/airflow/data/{country}/{region}/{dataset}/02_transform"

    return (
        download_dir,
        extract_dir,
        transform_dir,
        local_transform_dir,
        region_pbf_file,
        country_pbf_file,
        region_poly_file,
        cut_dir,
        cut_pbf_file,
    )


def load_to_adls_gen2(country: str, region: str, dataset: str, ds: str = "{{ ds }}"):
    """Upload parquet outputs from the Airflow container into ADLS Gen2.

    This function runs inside the Airflow container (PythonOperator), so it must
    reference the *container* path (`/opt/airflow/data/...`) produced by the Docker
    transform step.

    We upload both:
      - `{dataset}.parquet`        (WKB/geometry parquet)
      - `{dataset}_sf.parquet`     (Snowflake-friendly parquet)
    if they exist.
    """

    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient

    storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]
    account_url = f"https://{storage_account}.blob.core.windows.net"

    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    service = BlobServiceClient(account_url, credential=credential)
    container = service.get_container_client(ADLS_CONTAINER)

    # Compute container-local transform dir that should contain the parquets
    (
        _download_dir,
        _extract_dir,
        _transform_dir,
        local_transform_dir,
        _region_pbf_file,
        _country_pbf_file,
        _region_poly_file,
        _cut_dir,
        _cut_pbf_file,
    ) = make_paths(country, region, dataset, ds=ds)

    # Candidate local files to upload
    local_files = [
        os.path.join(local_transform_dir, f"{dataset}.parquet"),
        os.path.join(local_transform_dir, f"{dataset}_sf.parquet"),
    ]

    existing = [p for p in local_files if os.path.isfile(p)]
    if not existing:
        raise RuntimeError(
            "Expected parquet(s) not found. Checked: " + ", ".join(local_files)
        )

    # ADLS path prefix (keep it consistent with your stage root)
    # Example: raw/osm/germany/nordrhein-westfalen/roads/dt=2026-01-11/
    base_prefix = (ADLS_BASE_PATH or "").strip("/")
    remote_prefix = f"{base_prefix}/{country}/{region}/{dataset}/dt={ds}/" if base_prefix else f"{country}/{region}/{dataset}/dt={ds}/"

    uploaded = []
    for local_path in existing:
        blob_name = remote_prefix + os.path.basename(local_path)
        size_bytes = os.path.getsize(local_path)
        print(f"Uploading to ADLS: {blob_name} ({size_bytes} bytes)")

        # overwrite=True => idempotent
        with open(local_path, "rb") as fh:
            container.upload_blob(blob_name, fh, overwrite=True)

        uploaded.append(
            {
                "name": os.path.basename(local_path),
                "bytes": size_bytes,
                "blob": blob_name,
            }
        )

    return {
        "dataset": dataset,
        "country": country,
        "region": region,
        "ds": ds,
        "container": ADLS_CONTAINER,
        "remote_prefix": remote_prefix,
        "uploaded_files": len(uploaded),
        "files": uploaded,
    }


def build_osm_layer_dag(country: str, region: str, dataset: str) -> DAG:
    default_args = {
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    (
        download_dir,
        extract_dir,
        transform_dir,
        _local_transform_dir,
        region_pbf_file,
        country_pbf_file,
        region_poly_file,
        cut_dir,
        cut_pbf_file,
    ) = make_paths(country, region, dataset)

    source_mode = DATASET_SOURCE.get(dataset, "regional")
    use_osmium_cut = source_mode == "country_osmium_cut"
    pbf_for_extract = cut_pbf_file if use_osmium_cut else region_pbf_file

    mounts = [Mount(source=HOST_DATA_DIR, target="/data", type="bind")]

    ogr_layer = DATASET_EXTRACT[dataset]["ogr_layer"]
    where = DATASET_EXTRACT[dataset].get("where")

    # Defaults to avoid f-string brace issues and undefined variables.
    admin_levels_json = "[]"
    admin_osmium_export_json = ""
    admin_normalize_jq = ""
    admin_normalize_py = ""

    # Boundary/admin special-case: ensure we extract only relevant admin levels (default 2/4/6)
    # and keep the WHERE clause stable for non-admin datasets.
    if dataset == "admin":
        admin_levels = DATASET_EXTRACT[dataset].get("admin_levels", ["2", "4", "6"])
        # OGR OSM driver often exposes admin_level as an integer. Use numeric IN-list to avoid type mismatch.
        levels_sql = ",".join([str(int(lvl)) for lvl in admin_levels])
        # For jq filtering inside the admin extract branch
        admin_levels_json = "[" + ",".join([f'\"{lvl}\"' for lvl in admin_levels]) + "]"

        # These contain literal JSON/jq braces; keep them OUTSIDE f-strings.
        admin_osmium_export_json = '{ "attributes": { "type": "osm_type", "id": "osm_id" } }'

        admin_normalize_jq = r"""
        def hstore:
          to_entries
          | map("\"" + .key + "\"=>\"" + (.value|tostring|gsub("\\\""; "\\\\\\\"")) + "\"")
          | join(",");

        # Keep only admin relations with polygonal geometry.
        .properties as $p
        | ($p.admin_level|tostring) as $al
        | select($lvls | index($al))
        | ($p.osm_type // $p["@type"] // "") as $osm_t
        | select($osm_t == "relation")
        | select(.geometry != null)
        | select(.geometry.type == "Polygon" or .geometry.type == "MultiPolygon")
        | ($p.osm_id // $p["@id"] // $p.id) as $id
        | ($p.type // null) as $tag_type
        | ($p | del(.osm_id,.id,."@id",.osm_type,."@type",.name,.boundary,.admin_level)) as $rest
        | .properties = {
            osm_id: ($id|tostring),
            osm_type: $osm_t,
            type: $tag_type,
            name: ($p.name // null),
            boundary: ($p.boundary // null),
            admin_level: ($p.admin_level // null),
            other_tags: ($rest|hstore)
          }
        """

        admin_normalize_py = """import json, os, sys

raw = os.environ["RAW_FILE"]
out = os.environ["OUT_FILE"]
levels = set(json.loads(os.environ.get("LVLS", "[]")))

KEEP = {"osm_id", "id", "@id", "osm_type", "@type", "name", "boundary", "admin_level"}


def to_hstore(d: dict) -> str:
    # hstore-like: "k"=>"v",... with escaped quotes
    parts = []
    for k, v in d.items():
        if v is None:
            continue
        s = str(v).replace('"', '\\"')
        parts.append(f'"{k}"=>"{s}"')
    return ",".join(parts)


# Read as bytes to handle RFC7464 record separators (0x1E)
with open(raw, "rb") as f:
    data = f.read()

# Split records: prefer RS, else newline-delimited
records = data.split(b"\\x1e") if b"\\x1e" in data[:4096] else data.splitlines()

written = 0
with open(out, "w", encoding="utf-8") as fo:
    for rec in records:
        rec = rec.strip()
        if not rec:
            continue
        try:
            feat = json.loads(rec)
        except Exception:
            sys.stderr.write("[extract] ERROR: failed to parse a GeoJSONSeq record.\\n")
            sys.stderr.write(rec[:200].decode("utf-8", "replace") + "\\n")
            sys.exit(5)

        props = feat.get("properties") or {}
        al = str(props.get("admin_level") or "")
        if levels and al not in levels:
            continue

        osm_t = props.get("osm_type") or props.get("@type") or ""
        if osm_t != "relation":
            continue

        geom = feat.get("geometry") or {}
        if geom.get("type") not in ("Polygon", "MultiPolygon"):
            continue

        osm_id = props.get("osm_id") or props.get("@id") or props.get("id")
        if osm_id is None:
            continue

        tag_type = props.get("type")  # OSM tag type=boundary/multipolygon

        rest = {k: v for k, v in props.items() if k not in KEEP and k != "type"}

        feat["properties"] = {
            "osm_id": str(osm_id),
            "osm_type": osm_t,
            "type": tag_type,
            "name": props.get("name"),
            "boundary": props.get("boundary"),
            "admin_level": props.get("admin_level"),
            "other_tags": to_hstore(rest),
        }

        fo.write(json.dumps(feat, ensure_ascii=False))
        fo.write("\\n")
        written += 1

if written == 0:
    sys.stderr.write("[extract] ERROR: normalization produced 0 features (check filters).\\n")
    sys.exit(3)
"""

        base_where = where or "boundary='administrative'"
        # Avoid double-appending if user already put an admin_level filter in config
        if "admin_level" not in base_where.lower():
            base_where = f"{base_where} AND admin_level IN ({levels_sql})"

        # IMPORTANT: `where_clause` contains double quotes; do NOT embed it in echo-lines in bash.
        where_clause = f'-where "{base_where}"'
        where_debug = base_where
    else:
        where_clause = f'-where "{where}"' if where else ""
        where_debug = where or ""

    admin_filtered_pbf = f"{extract_dir}/admin_boundaries.osm.pbf"

    dag_id = f"osm_{dataset}_{country}_{region}"

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["osm", "geofabrik", "raw", dataset, country, region],
    ) as dag:

        clean = DockerOperator(
            task_id="clean_workspace",
            image="osm-tools:latest",
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=mounts,
            command=bash(
                f"""
                set -euo pipefail
                mkdir -p "{download_dir}" "{extract_dir}" "{transform_dir}"
                rm -rf "{extract_dir}" "{transform_dir}"
                mkdir -p "{extract_dir}" "{transform_dir}"
                """
            ),
        )

        download_inputs = DockerOperator(
            task_id="download_inputs",
            image="osm-tools:latest",
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=mounts,
            command=bash(
                f"""
                set -euo pipefail

                REGION_URL="{REGION_PBF_URL_TMPL.format(country=country, region=region)}"
                COUNTRY_URL="{COUNTRY_PBF_URL_TMPL.format(country=country)}"
                POLY_URL="{REGION_POLY_URL_TMPL.format(country=country, region=region)}"

                mkdir -p "{download_dir}"

                if [[ "{dataset}" == "admin" && "{source_mode}" == "country_osmium_cut" ]]; then
                  echo "[download] country pbf: $COUNTRY_URL"
                  curl -fL "$COUNTRY_URL" -o "{country_pbf_file}"

                  echo "[download] region poly: $POLY_URL"
                  curl -fL "$POLY_URL" -o "{region_poly_file}"
                else
                  echo "[download] regional pbf: $REGION_URL"
                  curl -fL "$REGION_URL" -o "{region_pbf_file}"
                fi
                """
            ),
        )

        osmium_cut_admin = None
        if use_osmium_cut:
            osmium_cut_admin = DockerOperator(
                task_id="osmium_cut_admin",
                image="osm-tools:latest",
                auto_remove=True,
                mount_tmp_dir=False,
                mounts=mounts,
                command=bash(
                    f"""
                    set -euo pipefail
                    mkdir -p "{cut_dir}"

                    # Strategy: smart; keep full region contents so member ways/nodes exist.
                    osmium extract \
                      -s smart \
                      -p "{region_poly_file}" \
                      "{country_pbf_file}" \
                      -o "{cut_pbf_file}" \
                      --overwrite

                    echo "[osmium] wrote: {cut_pbf_file}"
                    """
                ),
            )

        check_pbf = DockerOperator(
            task_id="check_pbf",
            image="osm-tools:latest",
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=mounts,
            command=bash(
                f"""
                set -euo pipefail
                test -f "{pbf_for_extract}"
                test $(stat -c%s "{pbf_for_extract}") -gt 10000
                """
            ),
        )

        extract = DockerOperator(
            task_id="extract",
            image="osm-tools:latest",
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=mounts,
            command=bash(
                f"""
                set -euo pipefail

                INPUT_PBF="{pbf_for_extract}"
                echo "[extract] INPUT_PBF=$INPUT_PBF"
                echo "[extract] dataset={dataset} ogr_layer={ogr_layer}"
                echo "[extract] where_sql={where_debug}"

                # Admin boundaries: we want complete relation geometry + stable ids.
                # 1) tags-filter relations we care about (referenced members are INCLUDED by default)
                # 2) osmium export builds geometry (no OGR OSM driver quirks)
                # 3) normalization (prefer python3, fallback to jq)
                if [[ "{dataset}" == "admin" ]]; then
                  # tags-filter relations we care about.
                  # NOTE: Some osmium versions don't support --add-referenced (and referenced objects are included by default).
                  # Try with --add-referenced; fall back if unsupported.
                  if ! osmium tags-filter "$INPUT_PBF" \
                        r/boundary=administrative \
                        r/type=boundary,multipolygon \
                        --add-referenced \
                        -o "{admin_filtered_pbf}" \
                        --overwrite
                  then
                    echo "[extract] --add-referenced not supported; retrying without it..."
                    osmium tags-filter "$INPUT_PBF" \
                      r/boundary=administrative \
                      r/type=boundary,multipolygon \
                      -o "{admin_filtered_pbf}" \
                      --overwrite
                  fi

                  osmium fileinfo -e "{admin_filtered_pbf}" || true
                  INPUT_PBF="{admin_filtered_pbf}"

                  RAW_FILE="{extract_dir}/{dataset}.raw.geojsonseq"
                  OUT_FILE="{extract_dir}/{dataset}.geojsonseq"

                  rm -f "$RAW_FILE" "$OUT_FILE"

                  # Export with id/type attributes (otherwise they are omitted by default)
                  cat > /tmp/osmium_export_admin.json <<'JSON'
{admin_osmium_export_json}
JSON

                  echo "[extract] osmium export -> $RAW_FILE"
                  osmium export "$INPUT_PBF" \
                    -c /tmp/osmium_export_admin.json \
                    -f geojsonseq \
                    -o "$RAW_FILE" \
                    --overwrite

                  # Allowed admin levels (string compare)
                  LVLS='{admin_levels_json}'

                  # Normalize schema + build other_tags.
                  # Prefer python3 (more robust for GeoJSONSeq record separators); fall back to jq.
                  if command -v python3 >/dev/null 2>&1; then
                    echo "[extract] Normalizing via python3..."
                    cat > /tmp/admin_normalize.py <<'PY'
{admin_normalize_py}
PY
                    RAW_FILE="$RAW_FILE" OUT_FILE="$OUT_FILE" LVLS="$LVLS" python3 /tmp/admin_normalize.py
                  else
                    if ! command -v jq >/dev/null 2>&1; then
                      echo "[extract] jq not found; installing..."
                      if command -v apt-get >/dev/null 2>&1; then
                        apt-get update -y && apt-get install -y jq
                      elif command -v apk >/dev/null 2>&1; then
                        apk add --no-cache jq
                      else
                        echo "[extract] ERROR: jq not found and no package manager (apt-get/apk) available" >&2
                        exit 9
                      fi
                    fi

                    cat > /tmp/admin_normalize.jq <<'JQ'
{admin_normalize_jq}
JQ

                    # GeoJSONSeq can be RFC7464 (0x1E record separators). Prefer jq --seq,
                    # but some outputs can still trip jq. Fall back by converting RS->NL.
                    if jq --seq -c --argjson lvls "$LVLS" -f /tmp/admin_normalize.jq "$RAW_FILE" > "$OUT_FILE"; then
                      :
                    else
                      echo "[extract] jq --seq failed; trying RS->newline fallback..." >&2
                      tr '\\036' '\\n' < "$RAW_FILE" | sed '/^[[:space:]]*$/d' | jq -c --argjson lvls "$LVLS" -f /tmp/admin_normalize.jq > "$OUT_FILE"
                    fi
                  fi

                else
                  # Non-admin datasets: regular OGR extract
                  OUT_FILE="{extract_dir}/{dataset}.geojsonseq"
                  ogr2ogr -f GeoJSONSeq "$OUT_FILE" \
                    "$INPUT_PBF" {ogr_layer} {where_clause}
                fi

                # Validate extract output exists and is non-empty (fail fast)
                if [[ ! -f "$OUT_FILE" ]]; then
                  echo "[extract] ERROR: output file not found: $OUT_FILE" >&2
                  echo "[extract] Listing extract_dir: {extract_dir}" >&2
                  ls -lah "{extract_dir}" || true
                  exit 2
                fi

                if [[ ! -s "$OUT_FILE" ]]; then
                  echo "[extract] ERROR: output file is empty: $OUT_FILE" >&2
                  echo "[extract] This usually means ogr2ogr WHERE clause matched 0 features." >&2
                  echo "[extract] ogr_layer={ogr_layer} where_sql={where_debug}" >&2
                  ls -lah "{extract_dir}" || true
                  exit 3
                fi

                echo "[extract] OK: wrote $(wc -l < "$OUT_FILE") GeoJSONSeq features to $OUT_FILE"
                head -n 2 "$OUT_FILE" || true
                """
            ),
        )

        transform = DockerOperator(
            task_id="transform",
            image="osm-tools:latest",
            auto_remove=True,
            mount_tmp_dir=False,
            mounts=mounts,
            command=bash(
                f"""
                set -euo pipefail

                IN_FILE="{extract_dir}/{dataset}.geojsonseq"
                echo "[transform] Checking input: $IN_FILE"
                ls -lah "{extract_dir}" || true
                if [[ ! -f "$IN_FILE" ]]; then
                  echo "[transform] ERROR: input file not found: $IN_FILE" >&2
                  exit 2
                fi
                if [[ ! -s "$IN_FILE" ]]; then
                  echo "[transform] ERROR: input file is empty: $IN_FILE" >&2
                  exit 3
                fi

                echo "[transform] input feature count: $(wc -l < \"$IN_FILE\")"
                echo "[transform] sample geom_wkt (first 3):"
                ogr2ogr -f CSV /vsistdout/ "$IN_FILE" \
                  -dialect SQLITE \
                  -sql "SELECT AsText(geometry) AS geom_wkt FROM {dataset} LIMIT 3" \
                  | head -n 5 || true

                echo "[transform] Writing WKB parquet: {transform_dir}/{dataset}.parquet"
                # 1) WKB parquet (geometry inside parquet)
                EXTRA_OPTS=""
                if [[ "{dataset}" == "admin" ]]; then
                  EXTRA_OPTS="-makevalid -skipfailures -nlt MULTIPOLYGON"
                fi

                ogr2ogr $EXTRA_OPTS -f Parquet "{transform_dir}/{dataset}.parquet" \
                  "{extract_dir}/{dataset}.geojsonseq" \
                  -lco COMPRESSION=ZSTD \
                  -lco ROW_GROUP_SIZE=50000

                echo "[transform] Writing Snowflake parquet: {transform_dir}/{dataset}_sf.parquet"
                # 2) Snowflake-friendly parquet (no geometry, only geom_wkt)
                ogr2ogr -makevalid -skipfailures \
                  -f Parquet "{transform_dir}/{dataset}_sf.parquet" \
                  "{extract_dir}/{dataset}.geojsonseq" \
                  -dialect SQLITE \
                  -sql "
                    SELECT
                        {SNOWFLAKE_COLUMNS[dataset]},
                        AsText(geometry) AS geom_wkt
                    FROM {dataset}
                  " \
                  -nlt NONE \
                  -lco GEOMETRY=NONE \
                  -lco COMPRESSION=ZSTD \
                  -lco ROW_GROUP_SIZE=50000
                """
            ),
        )

        load = PythonOperator(
            task_id="load_to_adls",
            python_callable=load_to_adls_gen2,
            op_kwargs={"country": country, "region": region, "dataset": dataset, "ds": "{{ ds }}"},
        )

        success = EmailOperator(
            task_id="success",
            to=EMAIL_TO,
            subject=(
                f"[SUCCESS] OSM {country}/{region} dataset={dataset} | dag={dag_id} | ds={{{{ ds }}}} | run={{{{ run_id }}}}"
            ),
            html_content=f"""
            <h2>✅ OSM pipeline succeeded</h2>

            <h3>Run info</h3>
            <ul>
              <li><b>DAG</b>: {dag_id}</li>
              <li><b>Dataset</b>: {dataset}</li>
              <li><b>Country/Region</b>: {country}/{region}</li>
              <li><b>ds</b>: {{{{ ds }}}}</li>
              <li><b>Run ID</b>: {{{{ run_id }}}}</li>
              <li><b>Logical date</b>: {{{{ logical_date }}}}</li>
              <li><b>Data interval</b>: {{{{ data_interval_start }}}} → {{{{ data_interval_end }}}}</li>
              <li><b>DAG start</b>: {{{{ dag_run.start_date }}}}</li>
              <li><b>DAG end</b>: {{{{ dag_run.end_date }}}}</li>
            </ul>

            <h3>Tasks</h3>
            <table border="1" cellpadding="6" cellspacing="0">
              <thead>
                <tr>
                  <th>task_id</th>
                  <th>state</th>
                  <th>try</th>
                  <th>start</th>
                  <th>end</th>
                  <th>duration (s)</th>
                  <th>log</th>
                </tr>
              </thead>
              <tbody>
              {{% for t in dag_run.get_task_instances() %}}
                <tr>
                  <td>{{{{ t.task_id }}}}</td>
                  <td>{{{{ t.state }}}}</td>
                  <td>{{{{ t.try_number }}}}</td>
                  <td>{{{{ t.start_date }}}}</td>
                  <td>{{{{ t.end_date }}}}</td>
                  <td>{{{{ t.duration }}}}</td>
                  <td><a href="{{{{ t.log_url }}}}">Open log</a></td>
                </tr>
              {{% endfor %}}
              </tbody>
            </table>

            <p>Airflow UI: <a href="{{{{ ti.log_url }}}}">Open this notification task log</a></p>
            """,
            conn_id="smtp_default",
        )

        failure = EmailOperator(
            task_id="failure",
            to=EMAIL_TO,
            subject=(
                f"[FAILED] OSM {country}/{region} dataset={dataset} | dag={dag_id} | ds={{{{ ds }}}} | run={{{{ run_id }}}}"
            ),
            html_content=f"""
            <h2>❌ OSM pipeline failed</h2>

            <h3>Run info</h3>
            <ul>
              <li><b>DAG</b>: {dag_id}</li>
              <li><b>Dataset</b>: {dataset}</li>
              <li><b>Country/Region</b>: {country}/{region}</li>
              <li><b>ds</b>: {{{{ ds }}}}</li>
              <li><b>Run ID</b>: {{{{ run_id }}}}</li>
              <li><b>Logical date</b>: {{{{ logical_date }}}}</li>
              <li><b>Data interval</b>: {{{{ data_interval_start }}}} → {{{{ data_interval_end }}}}</li>
              <li><b>DAG start</b>: {{{{ dag_run.start_date }}}}</li>
              <li><b>DAG end</b>: {{{{ dag_run.end_date }}}}</li>
            </ul>

            <h3>Tasks (states at time of notification)</h3>
            <table border="1" cellpadding="6" cellspacing="0">
              <thead>
                <tr>
                  <th>task_id</th>
                  <th>state</th>
                  <th>try</th>
                  <th>start</th>
                  <th>end</th>
                  <th>duration (s)</th>
                  <th>log</th>
                </tr>
              </thead>
              <tbody>
              {{% for t in dag_run.get_task_instances() %}}
                <tr>
                  <td>{{{{ t.task_id }}}}</td>
                  <td><b>{{{{ t.state }}}}</b></td>
                  <td>{{{{ t.try_number }}}}</td>
                  <td>{{{{ t.start_date }}}}</td>
                  <td>{{{{ t.end_date }}}}</td>
                  <td>{{{{ t.duration }}}}</td>
                  <td><a href="{{{{ t.log_url }}}}">Open log</a></td>
                </tr>
              {{% endfor %}}
              </tbody>
            </table>

            <p><b>Tip:</b> open logs for tasks with state <code>failed</code> / <code>upstream_failed</code>.</p>
            <p>Airflow UI: <a href="{{{{ ti.log_url }}}}">Open this notification task log</a></p>
            """,
            conn_id="smtp_default",
            trigger_rule=TriggerRule.ONE_FAILED,
        )

        if use_osmium_cut:
            clean >> download_inputs >> osmium_cut_admin >> check_pbf
        else:
            clean >> download_inputs >> check_pbf

        check_pbf >> extract >> transform >> load >> success

        failure_upstream = [clean, download_inputs, check_pbf, extract, transform, load]
        if use_osmium_cut and osmium_cut_admin is not None:
            failure_upstream.append(osmium_cut_admin)
        failure_upstream >> failure

    return dag