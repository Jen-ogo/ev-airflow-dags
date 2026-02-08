from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
import random
import time
import uuid
from typing import Any, Dict, List, Tuple

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# Databricks SQL connector
import databricks.sql as dbsql

from azure.eventhub import EventHubProducerClient, EventData

# Import EMAIL_TO from osm_config
from ev_repository.dags.osm.osm_config import EMAIL_TO

# ============================================================
# CONFIG
# ============================================================

DAG_ID = "tomtom_traffic_flowsegment_r7_hub"

# Databricks candidates source (prepared: candidate roads with centroid WKT)
DBX_TABLE_CANDIDATES = "geo_databricks_sub.GOLD.CANDIDATE_ROADS_R7"

# TomTom endpoint
TOMTOM_FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
TOMTOM_ZOOM = 10
TOMTOM_SERVICE_VERSION = "4"  # from URL: /traffic/services/4/


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Tuning
REQUEST_TIMEOUT_SEC = 20
SLEEP_BETWEEN_CALLS_SEC = 0.10
MAX_RETRIES = 5

# EventHub batching
EVENTHUB_BATCH_MAX_EVENTS = 200  # safety; EventHub has size-based limits too

# Parallelism (TomTom calls)
BATCH_SIZE = 50  # candidates per mapped task


def _debug_settings() -> Tuple[bool, int]:
    """Return (debug_mode, debug_max_rows). Read via Airflow Variables."""
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

    return dm, max(1, mr)


def _requests_get_json(url: str, params: Dict[str, Any], max_retries: int = MAX_RETRIES) -> Tuple[Dict[str, Any] | None, int | None, str | None]:
    """GET JSON with retry. Returns (json_or_none, http_status_or_none, error_message_or_none)."""
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
                snippet = None
                try:
                    snippet = (r.text or "").strip()
                    if len(snippet) > 500:
                        snippet = snippet[:500] + "…"
                except Exception:
                    snippet = None
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
    """Parse POINT(lon lat) from WKT; return (lon, lat)."""
    if not wkt:
        return None, None
    s = str(wkt).strip()
    if not s.upper().startswith("POINT(") or not s.endswith(")"):
        return None, None
    inner = s[len("POINT(") : -1].strip()
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


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 2, 8),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["traffic", "tomtom", "eventhub", "streaming", "r7"],
) as dag:

    # ============================================================
    # 1) LOAD REQUEST POINTS FROM DATABRICKS SQL
    # ============================================================
    @task
    def load_points_dbx() -> List[Dict[str, Any]]:
        """Fetch candidate road centroids from Databricks SQL Warehouse.

        Requires Airflow Connection: databricks_sql
          - host
          - password = PAT
          - extra.http_path
        """
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("databricks_sql")
        host = conn.host
        token = conn.password
        http_path = conn.extra_dejson.get("http_path")
        if not http_path:
            raise ValueError("databricks_sql.extra.http_path is required")

        sql = f"""
        select
          region_code,
          region,
          h3_r7,
          cast(degurba as int) as degurba,
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
        from {DBX_TABLE_CANDIDATES}
        where road_centroid_wkt_4326 is not null
          and road_centroid_wkt_4326 like 'POINT(%'
        """.strip()

        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

        points: List[Dict[str, Any]] = []
        for r in rows:
            rec = dict(zip(cols, r))
            lon, lat = _parse_point_wkt_lon_lat(rec.get("road_centroid_wkt_4326"))
            if lat is None or lon is None:
                continue
            rec["request_point_lat"] = lat
            rec["request_point_lon"] = lon
            points.append(rec)

        if not points:
            raise RuntimeError("❌ No valid candidate road centroids found in Databricks")

        debug_mode, debug_max_rows = _debug_settings()
        if debug_mode:
            points = points[:debug_max_rows]

        return points

    # ============================================================
    # 2) FEATURE FLAG
    # ============================================================
    @task
    def traffic_enabled(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return points if Variable.get("TRAFFIC_ENABLED", "false") == "true" else []

    # ============================================================
    # 3) SPLIT INTO BATCHES (DYNAMIC TASK MAPPING)
    # ============================================================
    @task
    def split_batches(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Split candidates into index-based batches for dynamic task mapping."""
        if not points:
            return []

        # Allow overriding via Airflow Variable if needed
        try:
            bs = int(Variable.get("TOMTOM_BATCH_SIZE", default_var=str(BATCH_SIZE)))
        except Exception:
            bs = BATCH_SIZE
        bs = max(1, bs)

        n = len(points)
        batches: List[Dict[str, Any]] = []
        batch_id = 1
        for start in range(0, n, bs):
            end = min(n, start + bs)
            batches.append({"batch_id": batch_id, "start_idx": start, "end_idx": end})
            batch_id += 1
        return batches

    # ============================================================
    # 4) FETCH TOMTOM TRAFFIC (MAPPED; PARALLEL)
    # ============================================================
    @task(pool="tomtom_api_pool")
    def fetch_traffic_batch(batch_id: int, start_idx: int, end_idx: int) -> Dict[str, Any]:
        """Fetch TomTom traffic for a slice of candidates.

        We intentionally pull the candidate list from XCom to keep mapping payload small.
        """
        ctx = get_current_context()
        ti = ctx["ti"]

        # pull enabled points (already filtered by feature-flag + debug limiting)
        points: List[Dict[str, Any]] = ti.xcom_pull(task_ids="traffic_enabled") or []
        if not points:
            return {"events": [], "api_calls": 0, "failed_calls": 0}

        s = int(start_idx)
        e = int(end_idx)
        pts = points[s:e]
        if not pts:
            return {"events": [], "api_calls": 0, "failed_calls": 0}

        # Always call the real API in this DAG (no dry-run mode).
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

            http_status = None
            error_message = None
            raw = None

            params = {"point": f"{lat},{lon}", "key": api_key}
            raw, http_status, error_message = _requests_get_json(TOMTOM_FLOW_URL, params)
            api_calls += 1
            if raw is None:
                failed_calls += 1

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

                # raw payload (string for EventHub stability)
                "raw_json": json.dumps(raw, ensure_ascii=False) if raw is not None else None,
            }

            events.append(event)

            # throttling to avoid burst
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        return {"events": events, "api_calls": api_calls, "failed_calls": failed_calls}

    # ============================================================
    # 4) PUSH TO EVENT HUB (BATCHED)
    # ============================================================
    @task
    def push_to_eventhub(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        # results is a list of dicts from mapped tasks
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

        if Variable.get("EVENTHUB_ENABLED", "false") != "true":
            return {"sent": 0, "api_calls": total_api_calls, "failed_calls": total_failed_calls}

        if not events:
            return {"sent": 0, "api_calls": total_api_calls, "failed_calls": total_failed_calls}

        producer = EventHubProducerClient.from_connection_string(
            Variable.get("EVENTHUB_CONN_STR"),
            eventhub_name=Variable.get("EVENTHUB_NAME"),
        )

        sent = 0
        with producer:
            batch = producer.create_batch()
            for e in events:
                try:
                    batch.add(EventData(json.dumps(e)))
                except ValueError:
                    # batch full by size; send current batch and start new
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(json.dumps(e)))

                sent += 1

                # optional safety: avoid huge batches even if size allows
                if sent % EVENTHUB_BATCH_MAX_EVENTS == 0:
                    producer.send_batch(batch)
                    batch = producer.create_batch()

            # send remainder
            if len(batch) > 0:
                producer.send_batch(batch)

        return {"sent": sent, "api_calls": total_api_calls, "failed_calls": total_failed_calls}


    # ============================================================
    # 5) NOTIFICATIONS (STANDARDIZED)
    # ============================================================

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject=(
            "[SUCCESS] TomTom Traffic flowSegment → EventHub | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>✅ TomTom Traffic flowSegment → EventHub succeeded</h2>

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
        {% set st = ti.xcom_pull(task_ids='push_to_eventhub') %}
        <ul>
          <li><b>TomTom API calls</b>: {{ st['api_calls'] if st and 'api_calls' in st else 'n/a' }}</li>
          <li><b>Failed TomTom calls</b>: {{ st['failed_calls'] if st and 'failed_calls' in st else 'n/a' }}</li>
          <li><b>Events sent to Event Hub</b>: {{ st['sent'] if st and 'sent' in st else 'n/a' }}</li>
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
        subject=(
            "[FAILED] TomTom Traffic flowSegment → EventHub | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>❌ TomTom Traffic flowSegment → EventHub failed</h2>

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
    points = load_points_dbx()
    enabled = traffic_enabled(points)
    batches = split_batches(enabled)
    traffic_batches = fetch_traffic_batch.expand_kwargs(batches)
    stats = push_to_eventhub(traffic_batches)

    # notifications
    stats >> success
    [points, enabled, batches, traffic_batches, stats] >> failure
