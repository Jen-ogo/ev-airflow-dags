from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

import snowflake.connector

from ev_repository.dags.osm.osm_config import EMAIL_TO

# ============================================================
# CONFIG
# ============================================================

DAG_ID = "tomtom_traffic_flowsegment_r7_hub_sf"

# --- Snowflake candidates selection ---
# We read *all* candidate roads directly from the Snowflake candidates table.
# (No TOP-N hex selection here.)

SF_TABLE_CANDIDATES = Variable.get(
    "SF_TRAFFIC_TABLE_CANDIDATES",
    default_var="GEO_PROJECT.GOLD.CANDIDATE_ROADS_R7",
)

# --- TomTom ---
TOMTOM_FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
TOMTOM_ZOOM = 10
TOMTOM_SERVICE_VERSION = "4"
REQUEST_TIMEOUT_SEC = 20
SLEEP_BETWEEN_CALLS_SEC = 0.10
MAX_RETRIES = 5

# --- EventHub Kafka endpoint (producer) ---
# bootstrap: "<NAMESPACE>.servicebus.windows.net:9093"
EVENTHUB_KAFKA_BOOTSTRAP = Variable.get(
    "EVENTHUB_KAFKA_BOOTSTRAP",
    default_var="geo-databricks-sub.servicebus.windows.net:9093",
)
EVENTHUB_TOPIC = Variable.get("EVENTHUB_KAFKA_TOPIC", default_var="traffic-flow-raw")

# SASL credentials:
# username MUST be literal "$ConnectionString"
EVENTHUB_SASL_USERNAME = "$ConnectionString"
# password MUST be full connection string:
# "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;"
EVENTHUB_SASL_PASSWORD = Variable.get("EVENTHUB_KAFKA_CONN_STR")

# Feature flags
TRAFFIC_ENABLED_VAR = "TRAFFIC_ENABLED"
KAFKA_ENABLED_VAR = "EVENTHUB_KAFKA_ENABLED"

# Parallelism
BATCH_SIZE = int(Variable.get("TOMTOM_BATCH_SIZE", default_var="50"))

DEFAULT_ARGS = {
    "owner": "geo",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# --- Snowflake RAW table (куда пишет Kafka Connect из Kafka/EventHub) ---
#   RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT, INSERTED_AT TIMESTAMP_NTZ
SF_ROW_TABLE = Variable.get(
    "SF_TRAFFIC_ROW_TABLE",
    default_var="GEO_PROJECT.GOLD.FACT_TOMTOM_TRAFFIC_FLOWSEGMENT_SNAPSHOTS_R7__KAFKA_RAW",
)

# ============================================================
# HELPERS
# ============================================================


# json.dumps helper for Decimal and datetime
def _json_default(o: Any):
    """json.dumps default handler.

    Snowflake connector often returns Decimal for NUMBER types.
    EventHub/Kafka payload must be JSON-serializable.
    """
    if isinstance(o, Decimal):
        # preserve ints when possible
        try:
            if o == o.to_integral_value():
                return int(o)
        except Exception:
            pass
        return float(o)

    # Be tolerant to datetime-like objects
    try:
        if isinstance(o, datetime):
            return o.isoformat()
    except Exception:
        pass

    return str(o)

def _debug_settings() -> Tuple[bool, int, float]:
    """Return (debug_mode, debug_max_rows, debug_sleep_sec).

    Read via Airflow Variables so we can switch without code changes:
      - TOMTOM_DEBUG_MODE: true/false (default false)
      - TOMTOM_DEBUG_MAX_ROWS: integer (default 10)
      - TOMTOM_DEBUG_SLEEP_SEC: float (default 0.0)
    """
    try:
        dm = str(Variable.get("TOMTOM_DEBUG_MODE", default_var="false")).strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
    except Exception:
        dm = False

    try:
        mr = int(Variable.get("TOMTOM_DEBUG_MAX_ROWS", default_var="10"))
    except Exception:
        mr = 10

    try:
        sl = float(Variable.get("TOMTOM_DEBUG_SLEEP_SEC", default_var="0"))
    except Exception:
        sl = 0.0

    return dm, max(1, mr), max(0.0, sl)

def _requests_get_json(url: str, params: Dict[str, Any], max_retries: int = MAX_RETRIES) -> Tuple[Dict[str, Any] | None, int | None, str | None]:
    last_err: Exception | None = None
    last_status: int | None = None

    retryable_statuses = {429, 500, 502, 503, 504}
    fail_fast_statuses = {400, 401, 403, 404}

    for i in range(max_retries):
        try:
            headers = {"User-Agent": "geo-airflow/1.0"}
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC, headers=headers)
            last_status = getattr(r, "status_code", None)

            if last_status in fail_fast_statuses:
                snippet = (r.text or "").strip()
                if len(snippet) > 500:
                    snippet = snippet[:500] + "…"
                msg = f"HTTP {last_status}: {snippet}" if snippet else f"HTTP {last_status}"
                return None, last_status, msg

            if last_status in retryable_statuses:
                time.sleep(0.5 * (2 ** i))
                continue

            r.raise_for_status()
            try:
                return r.json(), last_status, None
            except Exception as je:
                last_err = je
                time.sleep(0.5 * (2 ** i))
                continue

        except Exception as e:
            last_err = e
            try:
                last_status = getattr(getattr(e, "response", None), "status_code", last_status)
            except Exception:
                pass
            if last_status in fail_fast_statuses:
                return None, last_status, str(last_err)
            time.sleep(0.5 * (2 ** i))

    return None, last_status, str(last_err) if last_err is not None else "GET failed"


def _parse_point_wkt_lon_lat(wkt: str) -> Tuple[float | None, float | None]:
    """Parse POINT(lon lat) or POINT (lon lat) from WKT; return (lon, lat)."""
    if not wkt:
        return None, None

    s = str(wkt).strip()
    up = s.upper()
    if not up.startswith("POINT"):
        return None, None

    # Accept both "POINT(" and "POINT ("; find first '(' and last ')'
    try:
        i1 = s.find("(")
        i2 = s.rfind(")")
        if i1 < 0 or i2 < 0 or i2 <= i1:
            return None, None
        inner = s[i1 + 1 : i2].strip()
    except Exception:
        return None, None

    parts = inner.split()
    if len(parts) < 2:
        return None, None

    try:
        lon = float(parts[0])
        lat = float(parts[1])
    except Exception:
        return None, None

    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None, None

    return lon, lat


def _sf_conn_params_from_airflow_conn(conn_id: str = "sf_conn") -> Dict[str, Any]:
    """Use same pattern as your batch DAG: sf_conn in Airflow Connections."""
    from airflow.hooks.base import BaseHook
    sf = BaseHook.get_connection(conn_id)
    extra = sf.extra_dejson
    return {
        "user": sf.login,
        "password": sf.password,
        "account": extra.get("account"),
        "warehouse": extra.get("warehouse"),
        "database": extra.get("database"),
        "schema": extra.get("schema"),
        "role": extra.get("role"),
    }


def _sf_select(conn_params: Dict[str, Any], sql: str) -> List[Dict[str, Any]]:
    ctx = snowflake.connector.connect(**conn_params)
    try:
        with ctx.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        out = [dict(zip(cols, r)) for r in rows]
        return out
    finally:
        ctx.close()


def _produce_kafka_events(events: List[Dict[str, Any]]) -> int:
    """
    Producer to EventHub Kafka endpoint.
    Prefer confluent_kafka if available; fallback to kcat via subprocess.
    """
    if not events:
        return 0

    # Try confluent_kafka first (best)
    try:
        from confluent_kafka import Producer  # type: ignore

        conf = {
            "bootstrap.servers": EVENTHUB_KAFKA_BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": EVENTHUB_SASL_USERNAME,
            "sasl.password": EVENTHUB_SASL_PASSWORD,
            # a bit more stable defaults
            "client.id": f"airflow-{DAG_ID}",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "request.timeout.ms": 120000,
            "socket.timeout.ms": 120000,
        }

        p = Producer(conf)

        delivered = 0
        errors: List[str] = []

        def _cb(err, msg):
            nonlocal delivered
            if err is not None:
                errors.append(str(err))
            else:
                delivered += 1

        for e in events:
            payload = json.dumps(e, ensure_ascii=False, default=_json_default)
            p.produce(EVENTHUB_TOPIC, value=payload, callback=_cb)

        p.flush(60)

        if errors:
            # fail hard: better to see it
            raise RuntimeError("Kafka produce errors: " + "; ".join(errors[:5]))

        return delivered

    except ImportError:
        pass

    # Fallback: kcat (if installed in worker image)
    # NOTE: In many Airflow images `kcat` is not present or not executable for the airflow user.
    # We therefore resolve the absolute path and fail with a clear error if it cannot be executed.
    import subprocess
    import shutil

    kcat_path = shutil.which("kcat")
    if not kcat_path:
        raise RuntimeError(
            "Neither confluent_kafka nor kcat is available in the Airflow worker image. "
            "Install python package 'confluent-kafka' (recommended) or add 'kcat' to the image."
        )

    sent = 0
    for e in events:
        payload = json.dumps(e, ensure_ascii=False, default=_json_default)
        cmd = [
            kcat_path,
            "-b", EVENTHUB_KAFKA_BOOTSTRAP,
            "-t", EVENTHUB_TOPIC,
            "-P",
            "-X", "security.protocol=SASL_SSL",
            "-X", "sasl.mechanism=PLAIN",
            "-X", f"sasl.username={EVENTHUB_SASL_USERNAME}",
            "-X", f"sasl.password={EVENTHUB_SASL_PASSWORD}",
        ]

        try:
            proc = subprocess.run(
                cmd,
                input=payload.encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        except PermissionError as pe:
            raise RuntimeError(
                f"kcat exists but is not executable (permission denied): {kcat_path}. "
                "Fix: ensure the binary is executable for the airflow user (e.g. chmod +x), "
                "or install/use confluent-kafka." 
            ) from pe

        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="ignore")
            raise RuntimeError(f"kcat failed (rc={proc.returncode}): {err}")

        sent += 1

    return sent


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 2, 9),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["traffic", "tomtom", "eventhub", "kafka", "snowflake", "streaming", "r7"],
) as dag:

    @task
    def load_points_snowflake() -> List[Dict[str, Any]]:
        """
        Fetch candidates from Snowflake (Snowflake-native logic, not Databricks-style).

        Selection rule (as discussed):
          - Read candidate road points directly from the Snowflake candidates table.

        Expected minimum columns:
          - Hex table: H3_R7 plus one of: SCORE_GAP / MACRO_SCORE / MICRO_GAP
          - Roads table: REGION_CODE, REGION, H3_R7, ROAD_CENTROID_WKT_4326
        Optional context columns (passed through if present): DEGURBA, TRAFFIC_SCOPE, NEAR_EV_STATION,
          EV_STATION_ID, ROAD_FEATURE_ID, ROAD_OSM_ID, HIGHWAY, MAXSPEED_KPH, LANES, ROAD_LEN_M.
        """
        conn_params = _sf_conn_params_from_airflow_conn("sf_conn")

        sql = f"""
        SELECT
          region_code,
          region,
          h3_r7,
          TRY_TO_NUMBER(degurba) AS degurba,
          macro_score,
          traffic_scope,
          near_ev_station,
          ev_station_id,
          road_feature_id,
          road_osm_id,
          highway,
          maxspeed_kph,
          lanes,
          road_len_m,
          road_centroid_wkt_4326
        FROM {SF_TABLE_CANDIDATES}
        WHERE road_centroid_wkt_4326 IS NOT NULL
          AND (road_centroid_wkt_4326 LIKE 'POINT(%' OR road_centroid_wkt_4326 LIKE 'POINT (%' OR road_centroid_wkt_4326 ILIKE 'point(%' OR road_centroid_wkt_4326 ILIKE 'point (%')
        """.strip()

        rows = _sf_select(conn_params, sql)

        points: List[Dict[str, Any]] = []
        bad_wkts: List[str] = []

        for rec in rows:
            wkt = rec.get("ROAD_CENTROID_WKT_4326")
            lon, lat = _parse_point_wkt_lon_lat(wkt)
            if lat is None or lon is None:
                if wkt is not None and len(bad_wkts) < 5:
                    bad_wkts.append(str(wkt))
                continue

            # normalize keys to lower_snake (as in your hub DAG)
            out = {
                "region_code": rec.get("REGION_CODE"),
                "region": rec.get("REGION"),
                "h3_r7": rec.get("H3_R7"),
                "degurba": rec.get("DEGURBA"),
                "macro_score": rec.get("MACRO_SCORE"),
                "traffic_scope": rec.get("TRAFFIC_SCOPE"),
                "near_ev_station": rec.get("NEAR_EV_STATION"),
                "ev_station_id": rec.get("EV_STATION_ID"),
                "road_feature_id": rec.get("ROAD_FEATURE_ID"),
                "road_osm_id": rec.get("ROAD_OSM_ID"),
                "highway": rec.get("HIGHWAY"),
                "maxspeed_kph": rec.get("MAXSPEED_KPH"),
                "lanes": rec.get("LANES"),
                "road_len_m": rec.get("ROAD_LEN_M"),
                "road_centroid_wkt_4326": wkt,
                "request_point_lat": float(lat),
                "request_point_lon": float(lon),
            }
            points.append(out)

        if not points:
            raise RuntimeError(
                f"No valid candidates after Python WKT parsing from Snowflake table={SF_TABLE_CANDIDATES}. "
                f"Fetched rows={len(rows)}. Sample bad WKTs={bad_wkts}."
            )

        debug_mode, debug_max_rows, _debug_sleep = _debug_settings()
        if debug_mode:
            points = points[:debug_max_rows]

        return points


    @task
    def traffic_enabled(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return points if Variable.get(TRAFFIC_ENABLED_VAR, "false") == "true" else []


    @task
    def split_batches(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not points:
            return []
        bs = max(1, int(BATCH_SIZE))
        n = len(points)

        batches: List[Dict[str, Any]] = []
        batch_id = 1
        for start in range(0, n, bs):
            end = min(n, start + bs)
            batches.append({"batch_id": batch_id, "start_idx": start, "end_idx": end})
            batch_id += 1
        return batches


    @task(pool="tomtom_api_pool")
    def fetch_traffic_batch(batch_id: int, start_idx: int, end_idx: int, points: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not points:
            return {"events": [], "api_calls": 0, "failed_calls": 0}

        s = int(start_idx)
        e = int(end_idx)
        pts = points[s:e]
        if not pts:
            return {"events": [], "api_calls": 0, "failed_calls": 0}

        api_key = Variable.get("TOMTOM_TRAFFIC_API_KEY", default_var=None) or Variable.get("TOMTOM_API_KEY", default_var=None)
        if not api_key:
            raise RuntimeError("TomTom API key not found. Set TOMTOM_TRAFFIC_API_KEY (preferred) or TOMTOM_API_KEY.")

        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or "manual"
        snapshot_ts = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        events: List[Dict[str, Any]] = []
        api_calls = 0
        failed_calls = 0

        for p in pts:
            lat = float(p["request_point_lat"])
            lon = float(p["request_point_lon"])

            raw, http_status, error_message = _requests_get_json(TOMTOM_FLOW_URL, {"point": f"{lat},{lon}", "key": api_key})
            api_calls += 1
            if raw is None:
                failed_calls += 1

            # IMPORTANT:
            # - raw_json keep as OBJECT for Snowflake VARIANT (connector stores it nicely)
            # - event_id stable and searchable in RAW table
            event = {
                "event_id": str(uuid.uuid4()),
                "run_id": run_id,
                "snapshot_ts": snapshot_ts,
                "source": "TOMTOM",

                "tomtom_zoom": TOMTOM_ZOOM,
                "tomtom_version": TOMTOM_SERVICE_VERSION,

                "request_point_lat": lat,
                "request_point_lon": lon,
                "request_point_key": str(p.get("road_feature_id") or p.get("road_osm_id") or f"{lat},{lon}"),

                # candidate context
                "region_code": p.get("region_code"),
                "region": p.get("region"),
                "h3_r7": p.get("h3_r7"),
                "degurba": p.get("degurba"),
                "macro_score": p.get("macro_score"),
                "traffic_scope": p.get("traffic_scope"),
                "near_ev_station": p.get("near_ev_station"),
                "ev_station_id": p.get("ev_station_id"),

                "road_feature_id": p.get("road_feature_id"),
                "road_osm_id": p.get("road_osm_id"),
                "highway": p.get("highway"),
                "maxspeed_kph": p.get("maxspeed_kph"),
                "lanes": p.get("lanes"),
                "road_len_m": p.get("road_len_m"),
                "road_centroid_wkt_4326": p.get("road_centroid_wkt_4326"),

                # diagnostics
                "http_status": http_status,
                "error_message": error_message,

                # payload
                "raw_json": raw if raw is not None else {},
            }

            events.append(event)
            debug_mode, _debug_max_rows, debug_sleep_sec = _debug_settings()
            time.sleep(debug_sleep_sec if debug_mode else SLEEP_BETWEEN_CALLS_SEC)

        return {"events": events, "api_calls": api_calls, "failed_calls": failed_calls}


    @task
    def push_to_eventhub_kafka(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = results or []

        total_api_calls = 0
        total_failed_calls = 0
        events: List[Dict[str, Any]] = []

        for r in results:
            if not isinstance(r, dict):
                continue
            total_api_calls += int(r.get("api_calls", 0) or 0)
            total_failed_calls += int(r.get("failed_calls", 0) or 0)
            evs = r.get("events", []) or []
            if isinstance(evs, list) and evs:
                events.extend(evs)

        if Variable.get(KAFKA_ENABLED_VAR, "false") != "true":
            return {"sent": 0, "api_calls": total_api_calls, "failed_calls": total_failed_calls, "event_ids": []}

        if not events:
            return {"sent": 0, "api_calls": total_api_calls, "failed_calls": total_failed_calls, "event_ids": []}

        sent = _produce_kafka_events(events)
        event_ids = [e.get("event_id") for e in events if isinstance(e, dict) and e.get("event_id")]

        return {"sent": sent, "api_calls": total_api_calls, "failed_calls": total_failed_calls, "event_ids": event_ids}


    @task
    def validate_in_sf_raw(stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate that at least 1 event_id appears in Snowflake RAW table.
        RAW table structure assumed:
          RECORD_CONTENT (VARIANT/OBJECT), RECORD_METADATA (VARIANT), INSERTED_AT
        """
        if not stats or int(stats.get("sent", 0) or 0) <= 0:
            return {"validated": False, "reason": "nothing sent"}

        event_ids = stats.get("event_ids") or []
        event_ids = [x for x in event_ids if isinstance(x, str)]
        if not event_ids:
            return {"validated": False, "reason": "no event_ids"}

        # check first few ids
        check_ids = event_ids[:10]
        in_list = ", ".join([f"'{x}'" for x in check_ids])

        conn_params = _sf_conn_params_from_airflow_conn("sf_conn")
        sql = f"""
        SELECT COUNT(*) AS CNT
        FROM {SF_ROW_TABLE}
        WHERE RECORD_CONTENT:event_id::string IN ({in_list})
           OR RECORD_CONTENT:EVENT_ID::string IN ({in_list})
        """.strip()

        rows = _sf_select(conn_params, sql)
        cnt = int(rows[0]["CNT"]) if rows else 0
        if cnt <= 0:
            raise RuntimeError(
                f"Validation failed: no rows found in {SF_ROW_TABLE} for event_ids={check_ids}. "
                "Connector maybe lagging or mapping/topic mismatch."
            )

        return {"validated": True, "matched": cnt}


    # ============================================================
    # NOTIFICATIONS
    # ============================================================

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject="[SUCCESS] TomTom → EventHub(Kafka) → Snowflake RAW | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}",
        html_content="""
        <h2>✅ TomTom → EventHub(Kafka) → Snowflake RAW succeeded</h2>

        <h3>Run info</h3>
        <ul>
          <li><b>DAG</b>: {{ dag.dag_id }}</li>
          <li><b>ds</b>: {{ ds }}</li>
          <li><b>Run ID</b>: {{ run_id }}</li>
          <li><b>Logical date</b>: {{ logical_date }}</li>
          <li><b>Data interval</b>: {{ data_interval_start }} → {{ data_interval_end }}</li>
          <li><b>DAG start</b>: {{ dag_run.start_date }}</li>
          <li><b>DAG end</b>: {{ dag_run.end_date }}</li>
        </ul>

        <h3>Stats</h3>
        {% set st = ti.xcom_pull(task_ids='push_to_eventhub_kafka') %}
        {% set vd = ti.xcom_pull(task_ids='validate_in_sf_raw') %}
        <ul>
          <li><b>Sent to Kafka</b>: {{ st['sent'] if st and 'sent' in st else 'n/a' }}</li>
          <li><b>TomTom API calls</b>: {{ st['api_calls'] if st and 'api_calls' in st else 'n/a' }}</li>
          <li><b>Failed TomTom calls</b>: {{ st['failed_calls'] if st and 'failed_calls' in st else 'n/a' }}</li>
          <li><b>Validated in SF RAW</b>: {{ vd['validated'] if vd and 'validated' in vd else 'n/a' }}</li>
          <li><b>Matched rows</b>: {{ vd['matched'] if vd and 'matched' in vd else 'n/a' }}</li>
          <li><b>SF RAW table</b>: """ + str(SF_ROW_TABLE) + """</li>
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
          {% for t in dag_run.get_task_instances() %}
            <tr>
              <td>{{ t.task_id }}</td>
              <td>{{ t.state }}</td>
              <td>{{ t.try_number }}</td>
              <td>{{ t.start_date }}</td>
              <td>{{ t.end_date }}</td>
              <td>{{ t.duration }}</td>
              <td><a href="{{ t.log_url }}">Open log</a></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>

        <p>Airflow UI: <a href="{{ ti.log_url }}">Open this notification task log</a></p>
        """,
        conn_id="smtp_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure = EmailOperator(
        task_id="failure",
        to=EMAIL_TO,
        subject="[FAILED] TomTom → EventHub(Kafka) → Snowflake RAW | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}",
        html_content="""
        <h2>❌ TomTom → EventHub(Kafka) → Snowflake RAW failed</h2>

        <h3>Run info</h3>
        <ul>
          <li><b>DAG</b>: {{ dag.dag_id }}</li>
          <li><b>ds</b>: {{ ds }}</li>
          <li><b>Run ID</b>: {{ run_id }}</li>
          <li><b>Logical date</b>: {{ logical_date }}</li>
          <li><b>Data interval</b>: {{ data_interval_start }} → {{ data_interval_end }}</li>
          <li><b>DAG start</b>: {{ dag_run.start_date }}</li>
          <li><b>DAG end</b>: {{ dag_run.end_date }}</li>
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
          {% for t in dag_run.get_task_instances() %}
            <tr>
              <td>{{ t.task_id }}</td>
              <td><b>{{ t.state }}</b></td>
              <td>{{ t.try_number }}</td>
              <td>{{ t.start_date }}</td>
              <td>{{ t.end_date }}</td>
              <td>{{ t.duration }}</td>
              <td><a href="{{ t.log_url }}">Open log</a></td>
            </tr>
          {% endfor %}
          </tbody>
        </table>

        <p><b>Tip:</b> open logs for tasks with state <code>failed</code> / <code>upstream_failed</code>.</p>
        <p>Airflow UI: <a href="{{ ti.log_url }}">Open this notification task log</a></p>
        """,
        conn_id="smtp_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ============================================================
    # PIPELINE
    # ============================================================

    points = load_points_snowflake()
    enabled = traffic_enabled(points)
    batches = split_batches(enabled)

    # pass points into mapped task (so it doesn't pull XCom internally)
    traffic_batches = fetch_traffic_batch.partial(points=enabled).expand_kwargs(batches)

    stats = push_to_eventhub_kafka(traffic_batches)
    validated = validate_in_sf_raw(stats)

    validated >> success
    [points, enabled, batches, traffic_batches, stats, validated] >> failure