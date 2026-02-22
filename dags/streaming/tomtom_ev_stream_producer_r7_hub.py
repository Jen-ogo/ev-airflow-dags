from __future__ import annotations

import json
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ev_repository.dags.osm.osm_config import EMAIL_TO
import databricks.sql as dbsql

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


# =============================================================================
# CONFIG
# =============================================================================
# Source candidates (Databricks)
DBX_TABLE_CANDIDATES = "geo_databricks_sub.GOLD.FEAT_H3_MACRO_SCORE_R7"

# Candidate sampling / API tuning
TOP_CANDIDATES = 50
DEFAULT_LIMIT = 10  # POI hits per candidate
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.10

# Parallelism / dynamic mapping
# Each mapped task processes this many candidates.
BATCH_SIZE = int(os.getenv("EV_BATCH_SIZE", "10"))

# Producer batching strategy
# We DO NOT want to hold the full run in memory.
# Instead we emit small portions to Event Hub as soon as they are ready.
PRODUCE_CANDIDATES_PER_CHUNK = int(os.getenv("PRODUCE_CANDIDATES_PER_CHUNK", "10"))
# Safety flush: also flush if buffered events reach this count.
PRODUCE_EVENTS_PER_FLUSH = int(os.getenv("PRODUCE_EVENTS_PER_FLUSH", "25"))

TOMTOM_POI_URL = "https://api.tomtom.com/search/2/poiSearch/EV%20charging%20station.json"
TOMTOM_AVAIL_URL = "https://api.tomtom.com/search/2/chargingAvailability.json"
DEFAULT_TOMTOM_API_KEY = Variable.get("DEFAULT_TOMTOM_API_KEY", default_var=None) or os.getenv("DEFAULT_TOMTOM_API_KEY", "")

#
# NOTE: do NOT resolve secrets at import time.
# In Docker/Airflow, task runner and scheduler/webserver may have different env/secret backends.
# We resolve secrets inside the task.
#
# We also support multiple variable/env names to match different DAG/YAML conventions.
DEFAULT_EH_EVENTHUB_NAME_EV = os.getenv("EH_EVENTHUB_NAME_EV", "tomtom-ev-raw")

# ============================================================================
# EventHub settings helpers
# ============================================================================
def _get_first_non_empty(values: List[str]) -> str:
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _resolve_eventhub_settings() -> Dict[str, str]:
    """Resolve Event Hubs connection settings.

    Supports multiple Airflow Variable / env var names to stay compatible with
    existing conventions from other DAGs.

    Expected:
      - connection string at namespace level (preferred) OR with EntityPath
      - event hub entity name (optional if EntityPath is present)
    """

    # --- Connection string candidates (Variable -> env) ---
    conn_candidates = [
        Variable.get("EH_CONNECTION_STRING", default_var=None),
        Variable.get("EVENTHUB_CONNECTION_STRING", default_var=None),
        Variable.get("EVENTHUB_CONN_STR", default_var=None),
        Variable.get("EVENTHUB_CONN_STRING", default_var=None),
        os.getenv("EH_CONNECTION_STRING", ""),
        os.getenv("EVENTHUB_CONNECTION_STRING", ""),
        os.getenv("EVENTHUB_CONN_STR", ""),
        os.getenv("EVENTHUB_CONN_STRING", ""),
    ]

    eh_conn = _get_first_non_empty(conn_candidates)
    eh_conn = eh_conn.replace("\r", "").replace("\n", "").strip()

    # --- EventHub entity name candidates (Variable -> env) ---
    name_candidates = [
        Variable.get("EH_EVENTHUB_NAME_EV", default_var=None),
        Variable.get("EVENTHUB_NAME_EV", default_var=None),
        Variable.get("EVENTHUB_NAME", default_var=None),
        Variable.get("EV_EVENTHUB_NAME", default_var=None),
        os.getenv("EH_EVENTHUB_NAME_EV", ""),
        os.getenv("EVENTHUB_NAME_EV", ""),
        os.getenv("EVENTHUB_NAME", ""),
        os.getenv("EV_EVENTHUB_NAME", ""),
        DEFAULT_EH_EVENTHUB_NAME_EV,
    ]

    eh_name = _get_first_non_empty(name_candidates)

    if not eh_conn:
        raise ValueError(
            "Event Hubs connection string is missing. Set one of: "
            "Airflow Variable EH_CONNECTION_STRING / EVENTHUB_CONNECTION_STRING / EVENTHUB_CONN_STR "
            "or env var with the same name(s)."
        )

    # If the connection string already contains EntityPath, Azure SDK can work without eventhub_name.
    has_entity_path = "EntityPath=" in eh_conn

    if (not eh_name) and (not has_entity_path):
        raise ValueError(
            "Event Hub entity name is missing. Set one of: "
            "Airflow Variable EH_EVENTHUB_NAME_EV / EVENTHUB_NAME_EV / EVENTHUB_NAME / EV_EVENTHUB_NAME "
            "or env var with the same name(s), OR embed EntityPath=<hub-name> into the connection string."
        )

    return {"eh_conn": eh_conn, "eh_name": eh_name, "has_entity_path": str(has_entity_path)}

# Event Hub batch safety limits
EH_BATCH_MAX_BYTES = int(os.getenv("EH_BATCH_MAX_BYTES", "900000"))
EH_BATCH_MAX_EVENTS = int(os.getenv("EH_BATCH_MAX_EVENTS", "200"))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _radius_from_kring_area(kring_area_m2: float) -> int:
    if kring_area_m2 is None or float(kring_area_m2) <= 0:
        return 3000
    r = int(math.sqrt(float(kring_area_m2) / math.pi))
    return int(max(1500, min(10000, r)))


def _safe_params_for_log(params: Dict[str, Any]) -> Dict[str, Any]:
    """Return params with secrets redacted for logs."""
    if not params:
        return {}
    out = dict(params)
    if "key" in out:
        out["key"] = "***"
    return out


def _requests_get_json_safe(
    url: str,
    params: Dict[str, Any],
    max_retries: int = 5,
) -> Dict[str, Any]:
    """GET JSON with retries; never raises.

    Returns dict with keys:
      - ok: bool
      - status_code: int|None
      - json: dict|None
      - error: str|None
    """
    last_err: Exception | None = None
    last_status: int | None = None

    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            last_status = int(getattr(r, "status_code", 0) or 0)

            # Retry on transient statuses
            if last_status in (429, 500, 502, 503, 504):
                time.sleep(0.5 * (2 ** i))
                continue

            # Non-retriable errors (403/401/400 etc.)
            if last_status >= 400:
                # Keep the message short (avoid dumping big HTML)
                err_txt = (r.text or "").strip()
                if len(err_txt) > 300:
                    err_txt = err_txt[:300] + "..."
                return {
                    "ok": False,
                    "status_code": last_status,
                    "json": None,
                    "error": f"HTTP {last_status} for {url} params={_safe_params_for_log(params)} body={err_txt}",
                }

            # Success
            try:
                return {
                    "ok": True,
                    "status_code": last_status,
                    "json": r.json(),
                    "error": None,
                }
            except Exception as je:
                return {
                    "ok": False,
                    "status_code": last_status,
                    "json": None,
                    "error": f"Failed to decode JSON: {type(je).__name__}: {je}",
                }

        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))

    return {
        "ok": False,
        "status_code": last_status,
        "json": None,
        "error": f"GET failed after retries: {url} params={_safe_params_for_log(params)} err={type(last_err).__name__ if last_err else None}: {last_err}",
    }


def fetch_candidates_from_databricks_sql(**context):
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    if not http_path:
        raise ValueError("databricks_sql.extra.http_path is required")

    per_group = int(math.ceil(TOP_CANDIDATES / 3.0))

    sql = f"""
    with base as (
      select
        region_code,
        region,
        h3_r7,
        cell_center_wkt_4326,
        kring_area_m2,
        macro_score,
        degurba
      from {DBX_TABLE_CANDIDATES}
      where cell_center_wkt_4326 is not null
        and cell_center_wkt_4326 like 'POINT(%'
    ),
    ranked as (
      select
        region_code,
        region,
        h3_r7,
        cell_center_wkt_4326,
        kring_area_m2,
        macro_score,
        cast(degurba as int) as degurba,
        row_number() over (partition by cast(degurba as int) order by macro_score desc) as rn
      from base
      where degurba in (1, 2, 3)
    ),
    picked as (
      select *
      from ranked
      where rn <= {per_group}
    ),
    coords as (
      select
        region_code,
        region,
        h3_r7,
        degurba,
        try_cast(element_at(split(replace(replace(cell_center_wkt_4326, 'POINT(', ''), ')', ''), ' '), 1) as double) as lon,
        try_cast(element_at(split(replace(replace(cell_center_wkt_4326, 'POINT(', ''), ')', ''), ' '), 2) as double) as lat,
        kring_area_m2,
        macro_score
      from picked
    )
    select
      region_code,
      region,
      h3_r7,
      degurba,
      lon,
      lat,
      kring_area_m2,
      macro_score
    from coords
    where lat is not null and lon is not null
      and lat between -90 and 90
      and lon between -180 and 180
    order by macro_score desc
    limit {TOP_CANDIDATES}
    """

    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
        with c.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    df["radius_m"] = df["kring_area_m2"].apply(_radius_from_kring_area).astype(int)
    payload = df[["region_code", "region", "h3_r7", "degurba", "lat", "lon", "radius_m"]].to_dict("records")
    return payload


# ============================================================================
# TaskFlow dynamic mapping for parallel candidate batches
# ============================================================================
@task
def split_batches(candidates: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    """Split candidates into index-based batches for dynamic task mapping."""
    if not candidates:
        return []
    bs = max(1, int(BATCH_SIZE))
    n = len(candidates)
    out: List[Dict[str, int]] = []
    batch_id = 1
    for start in range(0, n, bs):
        end = min(n, start + bs)
        out.append({"batch_id": batch_id, "start_idx": start, "end_idx": end})
        batch_id += 1
    return out


@task
def produce_ev_batch_to_eventhub(batch_id: int, start_idx: int, end_idx: int) -> Dict[str, int]:
    """Process one candidate batch and immediately send envelopes to Event Hubs.

    This mirrors the traffic DAG behavior: each batch is sent as soon as it is ready.
    """
    import asyncio

    ctx = get_current_context()
    ti = ctx["ti"]

    # Pull full candidate list from XCom produced by `fetch_candidates_dbx` task
    candidates: List[Dict[str, Any]] = ti.xcom_pull(task_ids="fetch_candidates_dbx") or []
    if not candidates:
        return {"batch_id": int(batch_id), "candidates": 0, "envelopes": 0, "sent": 0, "tomtom_403": 0}

    s = int(start_idx)
    e = int(end_idx)
    chunk = candidates[s:e]
    if not chunk:
        return {"batch_id": int(batch_id), "candidates": 0, "envelopes": 0, "sent": 0, "tomtom_403": 0}

    tomtom_key = Variable.get("TOMTOM_API_KEY", default_var=None) or DEFAULT_TOMTOM_API_KEY
    if not tomtom_key:
        raise KeyError("TomTom API key not found")

    # Resolve Event Hubs settings at runtime
    eh_settings = _resolve_eventhub_settings()
    eh_conn = eh_settings["eh_conn"]
    eh_name = eh_settings["eh_name"]
    has_entity_path = (eh_settings.get("has_entity_path") == "True")

    airflow_dag_id = ctx["dag"].dag_id
    airflow_run_id = ctx["run_id"]
    airflow_try = int(ti.try_number)
    run_id = airflow_run_id  # keep consistent with other DAGs

    async def _run() -> Dict[str, int]:
        # If connection string already embeds EntityPath, `eventhub_name` may be omitted.
        if has_entity_path and not eh_name:
            producer = EventHubProducerClient.from_connection_string(conn_str=eh_conn)
        else:
            producer = EventHubProducerClient.from_connection_string(conn_str=eh_conn, eventhub_name=eh_name)

        envelopes: List[Dict[str, Any]] = []
        tomtom_403 = 0

        async with producer:
            for cand in chunk:
                lat = float(cand["lat"])
                lon = float(cand["lon"])
                radius_m = int(cand["radius_m"])

                poi_resp = _requests_get_json_safe(
                    TOMTOM_POI_URL,
                    {"key": tomtom_key, "lat": lat, "lon": lon, "radius": radius_m, "limit": DEFAULT_LIMIT},
                )
                if int(poi_resp.get("status_code") or 0) == 403:
                    tomtom_403 += 1

                poi_json = poi_resp.get("json") if poi_resp.get("ok") else None
                poi_results = (poi_json or {}).get("results", []) or []

                avail_list: List[Dict[str, Any]] = []
                for item in poi_results:
                    charging_av_id = (((item.get("dataSources") or {}).get("chargingAvailability") or {}).get("id"))
                    if not charging_av_id:
                        continue

                    avail_resp = _requests_get_json_safe(
                        TOMTOM_AVAIL_URL,
                        {"key": tomtom_key, "chargingAvailability": charging_av_id},
                    )
                    avail_list.append({
                        "chargingAvailabilityId": charging_av_id,
                        "ok": bool(avail_resp.get("ok")),
                        "status_code": avail_resp.get("status_code"),
                        "error": avail_resp.get("error"),
                        "avail_json": avail_resp.get("json") if avail_resp.get("ok") else None,
                    })
                    time.sleep(SLEEP_BETWEEN_CALLS_SEC)

                event_id = str(uuid.uuid4())
                snapshot_ts = _now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")

                envelopes.append({
                    "event_id": event_id,
                    "run_id": run_id,
                    "snapshot_ts": snapshot_ts,
                    "source": "TOMTOM",
                    "http_status": poi_resp.get("status_code"),
                    "error_message": poi_resp.get("error"),

                    "airflow_dag_id": airflow_dag_id,
                    "airflow_task_id": "produce_ev_batch_to_eventhub",
                    "airflow_run_id": airflow_run_id,
                    "airflow_try_number": airflow_try,

                    "region_code": cand.get("region_code"),
                    "region": cand.get("region"),
                    "h3_r7": cand.get("h3_r7"),
                    "degurba": int(cand["degurba"]) if cand.get("degurba") is not None else None,
                    "request_point_lat": lat,
                    "request_point_lon": lon,
                    "radius_m": radius_m,

                    "poi_json": poi_json,
                    "availability_snapshots": avail_list,
                })

                # Throttle between candidates
                time.sleep(SLEEP_BETWEEN_CALLS_SEC)

            sent = await _send_events_to_eventhub(producer, envelopes)

        return {
            "candidates": len(chunk),
            "envelopes": len(envelopes),
            "sent": int(sent),
            "tomtom_403": int(tomtom_403),
        }

    stats = asyncio.run(_run())

    # Log a compact summary per batch so it’s visible in Airflow logs
    print(
        f"[BATCH {batch_id}] candidates={stats['candidates']} envelopes={stats['envelopes']} sent={stats['sent']} tomtom_403={stats['tomtom_403']}"
    )

    return {"batch_id": int(batch_id), **stats}
@task
def summarize_batches(results: List[Dict[str, int]]) -> Dict[str, int]:
    results = results or []
    total_candidates = 0
    total_envelopes = 0
    total_sent = 0
    total_tomtom_403 = 0

    for r in results:
        if not isinstance(r, dict):
            continue
        total_candidates += int(r.get("candidates", 0) or 0)
        total_envelopes += int(r.get("envelopes", 0) or 0)
        total_sent += int(r.get("sent", 0) or 0)
        total_tomtom_403 += int(r.get("tomtom_403", 0) or 0)

    print(
        f"[SUMMARY] candidates={total_candidates} envelopes={total_envelopes} sent={total_sent} tomtom_403={total_tomtom_403}"
    )

    return {
        "total_candidates": total_candidates,
        "total_envelopes": total_envelopes,
        "total_sent": total_sent,
        "total_tomtom_403": total_tomtom_403,
    }


async def _send_events_to_eventhub(
    producer: EventHubProducerClient,
    events: List[Dict[str, Any]],
) -> int:
    """Send a list of JSON events to Event Hubs using an existing producer client.

    NOTE: azure-eventhub `EventDataBatch` does not expose `.count` in some versions.
    The stable way is to use `len(batch)`.
    """
    if not events:
        return 0

    produced = 0
    batch = await producer.create_batch()

    for e in events:
        body = json.dumps(e, ensure_ascii=False)
        ed = EventData(body)

        # Optional metadata (useful for debugging in Event Hubs capture / metrics)
        ed.properties = {
            "event_id": str(e.get("event_id", "")),
            "run_id": str(e.get("run_id", "")),
            "source": "TOMTOM_EV",
        }

        # Handle batch size limits
        try:
            batch.add(ed)
        except ValueError:
            # Current batch is full; flush and start a new batch.
            if len(batch) > 0:
                await producer.send_batch(batch)
                produced += len(batch)
            batch = await producer.create_batch()
            batch.add(ed)

        # Safety flush (bytes and event count)
        if batch.size_in_bytes > EH_BATCH_MAX_BYTES or len(batch) >= EH_BATCH_MAX_EVENTS:
            await producer.send_batch(batch)
            produced += len(batch)
            batch = await producer.create_batch()

    # Final flush
    if len(batch) > 0:
        await producer.send_batch(batch)
        produced += len(batch)

    return produced


def produce_ev_batches_to_eventhub(**context):
    """Fetch TomTom EV data and stream it to Event Hubs in small portions.

    Strategy:
      - Pull TOP_CANDIDATES from Databricks.
      - For each candidate, call TomTom POI + Availability.
      - Build ONE envelope per candidate.
      - Buffer envelopes and flush to Event Hubs frequently (portion-based).

    This keeps memory bounded and makes downstream Databricks consumer / DLT start
    receiving data immediately.
    """

    import asyncio

    # -------------------------------------------------------------------------
    # Inputs
    # -------------------------------------------------------------------------
    candidates: List[Dict[str, Any]] = context["ti"].xcom_pull(
        key="candidates", task_ids="fetch_candidates_dbx"
    )
    if not candidates:
        raise ValueError("No candidates pulled from Databricks")

    tomtom_key = Variable.get("TOMTOM_API_KEY", default_var=None) or DEFAULT_TOMTOM_API_KEY
    if not tomtom_key:
        raise KeyError("TomTom API key not found")

    # Resolve Event Hubs settings at runtime (Variables preferred; env fallback)
    eh_settings = _resolve_eventhub_settings()
    eh_conn = eh_settings["eh_conn"]
    eh_name = eh_settings["eh_name"]
    has_entity_path = (eh_settings.get("has_entity_path") == "True")

    # Debug-friendly (do NOT print secrets)
    if eh_name:
        print(f"[INFO] Using EventHub entity: {eh_name}")
    else:
        print("[INFO] Using EventHub entity from EntityPath in connection string")
    print(f"[INFO] EH_CONNECTION_STRING present: {bool(eh_conn)}")

    # Run metadata
    run_id = f"airflow__{_now_utc().isoformat()}"
    airflow_dag_id = context["dag"].dag_id
    airflow_run_id = context["run_id"]
    airflow_try = int(context["ti"].try_number)

    # -------------------------------------------------------------------------
    # Async runner (single EventHub producer per task)
    # -------------------------------------------------------------------------
    async def _run() -> Dict[str, int]:
        total_candidates = 0
        total_envelopes = 0
        total_sent = 0

        # If connection string already embeds EntityPath, `eventhub_name` may be omitted.
        if has_entity_path and not eh_name:
            producer = EventHubProducerClient.from_connection_string(conn_str=eh_conn)
        else:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=eh_conn,
                eventhub_name=eh_name,
            )

        buffer: List[Dict[str, Any]] = []

        async with producer:
            for idx, cand in enumerate(candidates, start=1):
                total_candidates += 1

                # -----------------------------------------------------------------
                # 1) Candidate -> TomTom POI Search
                # -----------------------------------------------------------------
                lat = float(cand["lat"])
                lon = float(cand["lon"])
                radius_m = int(cand["radius_m"])

                poi_resp = _requests_get_json_safe(
                    TOMTOM_POI_URL,
                    {"key": tomtom_key, "lat": lat, "lon": lon, "radius": radius_m, "limit": DEFAULT_LIMIT},
                )

                poi_json = poi_resp.get("json") if poi_resp.get("ok") else None
                poi_results = (poi_json or {}).get("results", []) or []

                # -----------------------------------------------------------------
                # 2) For each POI: Availability snapshot (if chargingAvailability id exists)
                # -----------------------------------------------------------------
                avail_list: List[Dict[str, Any]] = []
                for item in poi_results:
                    charging_av_id = (
                        ((item.get("dataSources") or {}).get("chargingAvailability") or {}).get("id")
                    )
                    if not charging_av_id:
                        continue

                    avail_resp = _requests_get_json_safe(
                        TOMTOM_AVAIL_URL,
                        {"key": tomtom_key, "chargingAvailability": charging_av_id},
                    )
                    avail_list.append({
                        "chargingAvailabilityId": charging_av_id,
                        "ok": bool(avail_resp.get("ok")),
                        "status_code": avail_resp.get("status_code"),
                        "error": avail_resp.get("error"),
                        "avail_json": avail_resp.get("json") if avail_resp.get("ok") else None,
                    })

                    time.sleep(SLEEP_BETWEEN_CALLS_SEC)

                # -----------------------------------------------------------------
                # 3) Envelope (ONE per candidate)
                # -----------------------------------------------------------------
                event_id = str(uuid.uuid4())
                snapshot_ts = _now_utc()

                envelope = {
                    "event_id": event_id,
                    "run_id": run_id,
                    "snapshot_ts": snapshot_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "source": "TOMTOM",
                    "http_status": poi_resp.get("status_code"),
                    "error_message": poi_resp.get("error"),

                    "airflow_dag_id": airflow_dag_id,
                    "airflow_task_id": "produce_ev_batches_to_eventhub",
                    "airflow_run_id": airflow_run_id,
                    "airflow_try_number": airflow_try,

                    "region_code": cand["region_code"],
                    "region": cand["region"],
                    "h3_r7": cand["h3_r7"],
                    "degurba": int(cand["degurba"]) if cand.get("degurba") is not None else None,
                    "request_point_lat": lat,
                    "request_point_lon": lon,
                    "radius_m": radius_m,

                    "poi_json": poi_json,
                    "availability_snapshots": avail_list,
                }

                # Keep the pipeline alive even when TomTom rejects the key or quotas.
                # We still emit the envelope (with http_status + error_message) so downstream can be tested.

                buffer.append(envelope)
                total_envelopes += 1

                # -----------------------------------------------------------------
                # 4) Portion-based flush
                #    Flush frequently to let the downstream consumer ingest immediately.
                # -----------------------------------------------------------------
                flush_by_candidate_chunk = (idx % PRODUCE_CANDIDATES_PER_CHUNK == 0)
                flush_by_event_count = (len(buffer) >= PRODUCE_EVENTS_PER_FLUSH)

                if flush_by_candidate_chunk or flush_by_event_count:
                    sent = await _send_events_to_eventhub(producer, buffer)
                    total_sent += sent
                    buffer = []

            # Final flush
            if buffer:
                sent = await _send_events_to_eventhub(producer, buffer)
                total_sent += sent

        return {
            "total_candidates": total_candidates,
            "total_envelopes": total_envelopes,
            "total_sent": total_sent,
        }

    stats = asyncio.run(_run())
    # NOTE: If you see http_status=401/403 in emitted envelopes, check TOMTOM_API_KEY (quota, key scope, referrer/IP restrictions).

    # -------------------------------------------------------------------------
    # XCom stats for email/debugging
    # -------------------------------------------------------------------------
    context["ti"].xcom_push(key="run_id", value=run_id)
    context["ti"].xcom_push(key="eh_eventhub_name", value=eh_name)
    context["ti"].xcom_push(key="total_candidates", value=int(stats["total_candidates"]))
    context["ti"].xcom_push(key="total_envelopes", value=int(stats["total_envelopes"]))
    context["ti"].xcom_push(key="total_sent", value=int(stats["total_sent"]))


with DAG(
    dag_id="tomtom_ev_stream_producer_r7_dbx",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "geo", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["geo", "tomtom", "ev", "eventhub", "databricks", "r7"],
) as dag:

    candidates = task(fetch_candidates_from_databricks_sql, task_id="fetch_candidates_dbx")()
    batches = split_batches(candidates)
    mapped = produce_ev_batch_to_eventhub.expand_kwargs(batches)
    summary = summarize_batches(mapped)

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject=(
            "[SUCCESS] TomTom EV -> EventHub (Databricks) | dag={{ dag.dag_id }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>✅ TomTom EV producer succeeded</h2>

        <h3>Run info</h3>
        <ul>
          <li><b>DAG</b>: {{ dag.dag_id }}</li>
          <li><b>Run ID</b>: {{ run_id }}</li>
          <li><b>Logical date</b>: {{ logical_date }}</li>
          <li><b>Data interval</b>: {{ data_interval_start }} → {{ data_interval_end }}</li>
          <li><b>DAG start</b>: {{ dag_run.start_date }}</li>
          <li><b>DAG end</b>: {{ dag_run.end_date }}</li>
        </ul>

        <h3>Stats</h3>
        {% set st = ti.xcom_pull(task_ids='summarize_batches') %}
        <ul>
          <li><b>candidates</b>: {{ st['total_candidates'] if st else 'n/a' }}</li>
          <li><b>envelopes</b>: {{ st['total_envelopes'] if st else 'n/a' }}</li>
          <li><b>sent to Event Hubs</b>: {{ st['total_sent'] if st else 'n/a' }}</li>
          <li><b>TomTom 403 count</b>: {{ st['total_tomtom_403'] if st else 'n/a' }}</li>
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
            "[FAILED] TomTom EV -> EventHub (Databricks) | dag={{ dag.dag_id }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>❌ TomTom EV producer failed</h2>

        <h3>Run info</h3>
        <ul>
          <li><b>DAG</b>: {{ dag.dag_id }}</li>
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
    summary >> success
    [candidates, batches, mapped, summary] >> failure