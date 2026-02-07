from __future__ import annotations

import base64
import json
import math
import os
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
import io
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.models.xcom_arg import XComArg
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Optional deps:
# pip install databricks-sql-connector snowflake-connector-python pandas pyarrow
import databricks.sql as dbsql
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from ev_repository.dags.osm.osm_config import EMAIL_TO

# ----------------------------
# Config
# ----------------------------

# Candidates source (Databricks) - already prepared: 50 candidates * roads
DBX_TABLE_CANDIDATES = "geo_databricks_sub.GOLD.CANDIDATE_ROADS_R7"

# Targets: Snowflake
SF_DB = "GEO_PROJECT"
SF_SCHEMA = "GOLD"
SF_FACT = f"{SF_DB}.{SF_SCHEMA}.FACT_TOMTOM_TRAFFIC_FLOWSEGMENT_SNAPSHOTS_R7"

# Targets: Databricks
DBX_CATALOG = "geo_databricks_sub"
DBX_SCHEMA = "GOLD"
DBX_FACT = f"{DBX_CATALOG}.{DBX_SCHEMA}.FACT_TOMTOM_TRAFFIC_FLOWSEGMENT_SNAPSHOTS_R7"

 # TomTom endpoint
TOMTOM_FLOW_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

# TomTom request metadata (not returned by API)
TOMTOM_ZOOM = 10
TOMTOM_SERVICE_VERSION = "4"  # from URL: /traffic/services/4/

# Tuning
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.10
MAX_RETRIES = 5

# Batching / parallelism
BATCH_SIZE = 50  # candidates per task
AIRFLOW_LANDING_BASEDIR = "/opt/airflow/data/tomtom_traffic_flowsegment_r7"  # must be a mounted volume


# Bootstrap key (if Airflow Variable not set)
# NOTE: Traffic API may require a different TomTom product/key than Search/EV POI.
# Prefer TOMTOM_TRAFFIC_API_KEY for this DAG, but fall back to TOMTOM_API_KEY for convenience.
DEFAULT_TOMTOM_API_KEY = "U5bhisKpXvV8TQAB74OgSaUBe8oECtIV"
TOMTOM_TRAFFIC_API_KEY_VAR = "TOMTOM_TRAFFIC_API_KEY"
TOMTOM_DEFAULT_API_KEY_VAR = "TOMTOM_API_KEY"


def _debug_settings() -> Tuple[bool, int, float]:
    """Return (debug_mode, debug_max_rows, debug_sleep_sec).

    Read via Airflow Variables so we can switch without code changes:
      - TOMTOM_DEBUG_MODE: true/false (default false)
      - TOMTOM_DEBUG_MAX_ROWS: integer (default 10)
      - TOMTOM_DEBUG_SLEEP_SEC: float (default 0.0)

    NOTE: Read inside task runtime (Variable access at parse time is risky).
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


# ----------------------------
# Helpers
# ----------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _landing_dir_for_run(run_id: str | None) -> str:
    """Return a per-run landing directory for batch outputs."""
    rid = (run_id or "manual").replace("/", "_").replace(":", "_").replace(" ", "_")
    return os.path.join(AIRFLOW_LANDING_BASEDIR, rid)


def _requests_get_json(
    url: str,
    params: Dict[str, Any],
    max_retries: int = MAX_RETRIES,
) -> Tuple[Dict[str, Any] | None, int | None, str | None]:
    """GET JSON with retry.

    Returns: (json_or_none, http_status_or_none, error_message_or_none)
    Never raises (so the caller can record a failed attempt into FACT).

    IMPORTANT:
      - Do NOT retry on auth/permission errors (401/403) — these will never succeed.
      - Include response body snippet in error_message for easier debugging.
    """
    last_err: Exception | None = None
    last_status: int | None = None

    # Status codes that are worth retrying (transient)
    retryable_statuses = {429, 500, 502, 503, 504}
    # Status codes that should fail fast (non-transient)
    fail_fast_statuses = {400, 401, 403, 404}

    for i in range(max_retries):
        try:
            # Some APIs behave better with a UA header; harmless otherwise
            headers = {"User-Agent": "geo-airflow/1.0"}
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT, headers=headers)
            last_status = getattr(r, "status_code", None)

            # Fail fast on non-transient errors
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

            # Retry on transient errors
            if last_status in retryable_statuses:
                time.sleep(0.5 * (2 ** i))
                continue

            # Raise for other non-2xx
            r.raise_for_status()

            # Parse JSON
            try:
                return r.json(), last_status, None
            except Exception as je:
                last_err = je
                # JSON parse errors: try a couple times, but don't spin forever
                time.sleep(0.5 * (2 ** i))
                continue

        except Exception as e:
            last_err = e
            # status might be available on HTTPError via response
            try:
                last_status = getattr(getattr(e, "response", None), "status_code", last_status)
            except Exception:
                pass
            # If we can determine it's auth/forbidden, fail fast
            if last_status in fail_fast_statuses:
                return None, last_status, str(last_err)
            time.sleep(0.5 * (2 ** i))

    return None, last_status, str(last_err) if last_err is not None else "GET failed"



def _make_linestring_wkt(coords: List[Dict[str, Any]]) -> str | None:
    """
    TomTom coords are [{latitude, longitude}, ...]
    Return: LINESTRING(lon lat, lon lat, ...)
    """
    if not coords or not isinstance(coords, list):
        return None
    pts = []
    for c in coords:
        if not isinstance(c, dict):
            continue
        lat = c.get("latitude")
        lon = c.get("longitude")
        if lat is None or lon is None:
            continue
        try:
            lat = float(lat)
            lon = float(lon)
        except Exception:
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        pts.append(f"{lon} {lat}")
    if len(pts) < 2:
        return None
    return "LINESTRING(" + ", ".join(pts) + ")"


# Helper: coerce to bool
def _to_bool(v: Any) -> bool | None:
    """Coerce common boolean-ish values to real bool.

    Handles: True/False, 1/0, 1.0/0.0, "true"/"false", "1"/"0".
    Returns None for null/NaN/unknown.
    """
    if v is None:
        return None
    # pandas / numpy NaN
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass

    if isinstance(v, bool):
        return v

    # numeric
    if isinstance(v, (int, float)):
        try:
            fv = float(v)
        except Exception:
            return None
        if fv == 1.0:
            return True
        if fv == 0.0:
            return False
        return None

    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "yes", "y", "1"):
            return True
        if s in ("false", "f", "no", "n", "0"):
            return False
        return None

    return None


# Timestamp normalization/clamping (reuse the EV approach)
def _coerce_ts_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)

    def _epoch_to_dt(x: float) -> datetime | None:
        try:
            ax = abs(float(x))
        except Exception:
            return None
        try:
            if ax >= 1e18:
                secs = float(x) / 1e9
            elif ax >= 1e15:
                secs = float(x) / 1e6
            elif ax >= 1e12:
                secs = float(x) / 1e3
            else:
                secs = float(x)
            return datetime.fromtimestamp(secs, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    if isinstance(v, dict):
        if "seconds_since_epoch" in v:
            return _epoch_to_dt(v.get("seconds_since_epoch"))
        try:
            dt = pd.to_datetime(v, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    if isinstance(v, (int, float)):
        return _epoch_to_dt(v)

    if isinstance(v, str):
        s = v.strip()

        # unwrap stringified tuples like "(seconds_since_epoch=..., nanos=...)"
        if s.startswith("(") and s.endswith(")") and len(s) > 2:
            inner = s[1:-1].strip()
            # recurse to reuse the same parsing logic
            return _coerce_ts_value(inner)

        # Stringified Spark/Arrow-style struct
        if "seconds_since_epoch" in s:
            # be tolerant to formats like:
            #  - seconds_since_epoch=123
            #  - seconds_since_epoch: 123
            #  - seconds_since_epoch 123
            m = re.search(r"seconds_since_epoch\s*[:=]\s*([0-9]+)", s)
            if not m:
                m = re.search(r"seconds_since_epoch\s+([0-9]+)", s)
            if m:
                return _epoch_to_dt(float(m.group(1)))

        # numeric string => treat as epoch
        if s and all(ch.isdigit() or ch in "+-." for ch in s) and any(ch.isdigit() for ch in s):
            try:
                return _epoch_to_dt(float(s))
            except Exception:
                return None

        # ISO / other timestamp strings
        try:
            dt = pd.to_datetime(s, errors="coerce", utc=True)
            if pd.isna(dt):
                return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    return None


def _normalize_timestamp_columns(df: pd.DataFrame, cols: Tuple[str, ...] = ("LOAD_TS", "SNAPSHOT_TS")) -> None:
    """In-place normalize timestamp columns to pandas datetime64[ns] (naive).

    IMPORTANT: works case-insensitively (LOAD_TS vs load_ts), because we sometimes
    lowercase columns for Delta compatibility.
    """
    if df is None or len(df) == 0:
        return

    # map lowercase -> actual name
    col_map = {str(c).lower(): c for c in df.columns}

    for c in cols:
        key = str(c).lower()
        actual = col_map.get(key)
        if actual is None:
            continue

        df[actual] = df[actual].apply(_coerce_ts_value)
        df[actual] = pd.to_datetime(df[actual], errors="coerce").dt.tz_localize(None)


def _clamp_timestamp_columns(
    df: pd.DataFrame,
    cols: Tuple[str, ...] = ("LOAD_TS", "SNAPSHOT_TS"),
    min_ts: str = "2000-01-01",
    max_ts: str = "2100-01-01",
    on_out_of_range: str = "now",
) -> None:
    if df is None or len(df) == 0:
        return
    min_dt = pd.Timestamp(min_ts)
    max_dt = pd.Timestamp(max_ts)
    now_dt = pd.Timestamp(_now_utc().replace(tzinfo=None))
    col_map = {str(c).lower(): c for c in df.columns}

    for c in cols:
        actual = col_map.get(str(c).lower())
        if actual is None:
            continue

        df[actual] = pd.to_datetime(df[actual], errors="coerce")
        mask_bad = df[actual].isna() | (df[actual] < min_dt) | (df[actual] > max_dt)
        if mask_bad.any():
            df.loc[mask_bad, actual] = now_dt if on_out_of_range == "now" else pd.NaT


# ----------------------------
# Databricks: fetch candidates
# ----------------------------
def fetch_candidates_from_databricks_sql(**context):
    """
    Pull candidate road centroids from Databricks SQL Warehouse.
    Requires Airflow Connection: databricks_sql (host, password=PAT, extra.http_path).
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    if not http_path:
        raise ValueError("databricks_sql.extra.http_path is required")

    sql = f"""
    with src as (
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
    ),
    coords as (
      select
        *,
        try_cast(element_at(split(replace(replace(road_centroid_wkt_4326, 'POINT(', ''), ')', ''), ' '), 1) as double) as lon,
        try_cast(element_at(split(replace(replace(road_centroid_wkt_4326, 'POINT(', ''), ')', ''), ' '), 2) as double) as lat,
        highway,
        maxspeed_kph,
        lanes,
        road_len_m
      from src
    )
    select
      region_code,
      region,
      h3_r7,
      degurba,
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
      road_centroid_wkt_4326,
      lat,
      lon
    from coords
    where lat is not null and lon is not null
      and lat between -90 and 90
      and lon between -180 and 180
    """

    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
        with c.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    payload = df.to_dict("records")

    # Debug mode: limit how many road-candidates we will call TomTom for
    debug_mode, debug_max_rows, _debug_sleep = _debug_settings()
    if debug_mode:
        payload = payload[:debug_max_rows]

    context["ti"].xcom_push(key="candidates", value=payload)
    return payload


def split_candidates_into_batches(**context) -> List[Dict[str, Any]]:
    """Split candidate list into index-based batches.

    Returns a list of dicts suitable for dynamic task mapping via `expand(op_kwargs=...)`.
    We only pass (batch_id, start_idx, end_idx) to avoid huge XCom payloads.
    """
    candidates = context["ti"].xcom_pull(task_ids="fetch_candidates_dbx")
    if not candidates:
        candidates = context["ti"].xcom_pull(key="candidates", task_ids="fetch_candidates_dbx")

    if not candidates:
        raise ValueError("No candidates found to batch")

    n = len(candidates)

    debug_mode, debug_max_rows, _ = _debug_settings()
    if debug_mode:
        n = min(n, debug_max_rows)

    batches: List[Dict[str, Any]] = []
    batch_id = 1
    for start in range(0, n, BATCH_SIZE):
        end = min(n, start + BATCH_SIZE)
        batches.append({"batch_id": batch_id, "start_idx": start, "end_idx": end})
        batch_id += 1

    return batches


# ----------------------------
# TomTom enrichment
# ----------------------------

def enrich_candidates_with_tomtom_traffic_batch(batch_id: int, start_idx: int, end_idx: int, **context) -> str:
    """Enrich a slice of candidates with TomTom traffic and write results to a JSON file.

    Returns the local file path (string). This keeps XCom small.

    IMPORTANT: timestamp normalization/clamping stays identical to the working approach.
    """
    # Prefer a dedicated Traffic API key/variable; fall back to the default TOMTOM_API_KEY.
    tomtom_key = Variable.get(TOMTOM_TRAFFIC_API_KEY_VAR, default_var=None)
    if not tomtom_key:
        tomtom_key = Variable.get(TOMTOM_DEFAULT_API_KEY_VAR, default_var=None)

    if not tomtom_key:
        # bootstrap (only if absolutely missing)
        tomtom_key = DEFAULT_TOMTOM_API_KEY
        try:
            Variable.set(TOMTOM_DEFAULT_API_KEY_VAR, tomtom_key)
        except Exception:
            pass

    if not tomtom_key:
        raise KeyError(
            f"TomTom API key not found. Set Airflow Variable {TOMTOM_TRAFFIC_API_KEY_VAR} (preferred) "
            f"or {TOMTOM_DEFAULT_API_KEY_VAR}."
        )

    # Pull all candidates (small enough) and slice by indices to avoid huge mapping args
    all_candidates: List[Dict[str, Any]] = context["ti"].xcom_pull(task_ids="fetch_candidates_dbx")
    if not all_candidates:
        all_candidates = context["ti"].xcom_pull(key="candidates", task_ids="fetch_candidates_dbx")

    if not all_candidates:
        raise ValueError("No candidates pulled from Databricks")

    candidates = all_candidates[int(start_idx) : int(end_idx)]

    # Add run_id for request metadata
    run_id = context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else None)

    rows: List[Dict[str, Any]] = []
    load_ts = _now_utc().replace(tzinfo=None)

    # Debug settings affect sleep only (batch slicing is already bounded)
    debug_mode, _debug_max_rows, debug_sleep_sec = _debug_settings()

    for cand in candidates:
        lat = float(cand["lat"])
        lon = float(cand["lon"])

        params = {
            "key": tomtom_key,
            "point": f"{lat},{lon}",
        }

        raw, http_status, error_message = _requests_get_json(TOMTOM_FLOW_URL, params)
        if error_message is not None and len(error_message) > 800:
            error_message = error_message[:800] + "…"

        fsd = (raw or {}).get("flowSegmentData") or {}

        coords = ((fsd.get("coordinates") or {}).get("coordinate")) or []
        linestring_wkt = _make_linestring_wkt(coords)

        def _to_int(v: Any) -> int | None:
            if v is None:
                return None
            try:
                return int(float(v))
            except Exception:
                return None

        current_speed = _to_int(fsd.get("currentSpeed"))
        free_flow_speed = _to_int(fsd.get("freeFlowSpeed"))
        current_travel_time = _to_int(fsd.get("currentTravelTime"))
        free_flow_travel_time = _to_int(fsd.get("freeFlowTravelTime"))

        speed_ratio = (
            (float(current_speed) / float(free_flow_speed))
            if current_speed not in (None, 0) and free_flow_speed not in (None, 0)
            else None
        )
        delay_sec = (
            (current_travel_time - free_flow_travel_time)
            if current_travel_time is not None and free_flow_travel_time is not None
            else None
        )
        delay_ratio = (
            (float(current_travel_time) / float(free_flow_travel_time))
            if current_travel_time not in (None, 0) and free_flow_travel_time not in (None, 0)
            else None
        )

        rows.append(
            {
                # run/request metadata (not returned by API)
                "RUN_ID": run_id,
                "REQUEST_POINT_KEY": str(cand.get("road_feature_id") or cand.get("road_osm_id") or f"{lat},{lon}"),
                "TOMTOM_ZOOM": TOMTOM_ZOOM,
                "TOMTOM_VERSION": TOMTOM_SERVICE_VERSION,

                # candidate identity
                "REGION_CODE": cand.get("region_code"),
                "REGION": cand.get("region"),
                "H3_R7": cand.get("h3_r7"),
                "DEGURBA": cand.get("degurba"),
                "MACRO_SCORE": cand.get("macro_score"),
                "TRAFFIC_SCOPE": cand.get("traffic_scope"),
                "NEAR_EV_STATION": _to_bool(cand.get("near_ev_station")),
                "EV_STATION_ID": cand.get("ev_station_id"),
                "ROAD_FEATURE_ID": cand.get("road_feature_id"),
                "ROAD_OSM_ID": cand.get("road_osm_id"),
                "HIGHWAY": cand.get("highway"),
                "MAXSPEED_KPH": cand.get("maxspeed_kph"),
                "LANES": cand.get("lanes"),
                "ROAD_LEN_M": cand.get("road_len_m"),
                "ROAD_CENTROID_WKT_4326": cand.get("road_centroid_wkt_4326"),

                # request metadata
                "REQUEST_POINT_LAT": lat,
                "REQUEST_POINT_LON": lon,
                "HTTP_STATUS": http_status,
                "ERROR_MESSAGE": error_message,
                "SNAPSHOT_TS": load_ts,
                "LOAD_TS": load_ts,

                # flowSegment scalars
                "FRC": fsd.get("frc"),
                "CURRENT_SPEED": current_speed,
                "FREE_FLOW_SPEED": free_flow_speed,
                "CURRENT_TRAVEL_TIME": current_travel_time,
                "FREE_FLOW_TRAVEL_TIME": free_flow_travel_time,
                "CONFIDENCE": fsd.get("confidence"),
                "ROAD_CLOSURE": _to_bool(fsd.get("roadClosure")),

                # derived metrics
                "SPEED_RATIO": speed_ratio,
                "DELAY_SEC": delay_sec,
                "DELAY_RATIO": delay_ratio,

                # geometry / raw
                "SEGMENT_LINESTRING_WKT_4326": linestring_wkt,
                "SEGMENT_COORDS_JSON": coords if coords is not None else None,

                # keep raw response (SF can store VARIANT; DBX we will stringify)
                "RAW_JSON": raw,
            }
        )

        # Sleep control
        if debug_mode:
            time.sleep(debug_sleep_sec)
        else:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    fact_df = pd.DataFrame(rows)

    # timestamps normalize + clamp (defensive)
    _normalize_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"))
    _clamp_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"), on_out_of_range="now")

    # Serialize timestamps as strings for safe downstream loads
    for c in ("LOAD_TS", "SNAPSHOT_TS"):
        if c in fact_df.columns:
            fact_df[c] = fact_df[c].apply(_coerce_ts_value)
            fact_df[c] = pd.to_datetime(fact_df[c], errors="coerce").dt.tz_localize(None)
            _clamp_timestamp_columns(fact_df, cols=(c,), on_out_of_range="now")
            fact_df[c] = pd.to_datetime(fact_df[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # Write batch output to a per-run landing dir
    landing_dir = _landing_dir_for_run(run_id)
    os.makedirs(landing_dir, exist_ok=True)

    out_path = os.path.join(landing_dir, f"fact_batch_{int(batch_id):04d}_{int(start_idx):06d}_{int(end_idx):06d}.json")
    fact_df.to_json(out_path, orient="records", force_ascii=False)

    return out_path


# ----------------------------
# Snowflake load
# ----------------------------
def load_to_snowflake(batch_files: List[str], **context):
    """Load traffic FACT into Snowflake.

    IMPORTANT: follow the same approach as the working EV enrichment DAG:
      - normalize+clamp timestamps
      - then serialize LOAD_TS/SNAPSHOT_TS to plain strings BEFORE write_pandas

    This avoids Snowflake errors like:
      Timestamp '(seconds_since_epoch=...)' is not recognized
    """
    from airflow.hooks.base import BaseHook

    if not batch_files:
        raise ValueError("No batch files provided to load_to_snowflake")

    parts: List[pd.DataFrame] = []
    for p in batch_files:
        if not p:
            continue
        with open(p, "r", encoding="utf-8") as f:
            s = f.read()
        if not s.strip():
            continue
        parts.append(pd.read_json(io.StringIO(s), orient="records"))

    if not parts:
        raise ValueError("All batch files were empty; nothing to load")

    fact_df = pd.concat(parts, ignore_index=True)

    # 1) Normalize + clamp timestamps coming from XCom JSON
    _normalize_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"))
    _clamp_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"), on_out_of_range="now")

    # 2) Ensure BOOLEAN columns are real bool/None
    for bc in ("NEAR_EV_STATION", "ROAD_CLOSURE", "near_ev_station", "road_closure"):
        if bc in fact_df.columns:
            fact_df[bc] = fact_df[bc].apply(_to_bool)

    # 3) FINAL: force Snowflake timestamp columns to plain strings (EV pattern)
    #    - also handles any weird struct-like representations via _coerce_ts_value
    for c in ("LOAD_TS", "SNAPSHOT_TS"):
        if c in fact_df.columns:
            fact_df[c] = fact_df[c].apply(_coerce_ts_value)
            fact_df[c] = pd.to_datetime(fact_df[c], errors="coerce").dt.tz_localize(None)
            # clamp again after coercion (defensive)
            _clamp_timestamp_columns(fact_df, cols=(c,), on_out_of_range="now")
            fact_df[c] = pd.to_datetime(fact_df[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # 4) Snowflake connection
    sf = BaseHook.get_connection("sf_conn")
    extra = sf.extra_dejson

    ctx = snowflake.connector.connect(
        user=sf.login,
        password=sf.password,
        account=extra.get("account"),
        warehouse=extra.get("warehouse"),
        database=extra.get("database", SF_DB),
        schema=extra.get("schema", SF_SCHEMA),
        role=extra.get("role"),
    )

    # 5) Ensure the Snowflake FACT table has all required columns (idempotent)
    required_cols = [
        ("RUN_ID", "VARCHAR"),
        ("SNAPSHOT_TS", "TIMESTAMP_NTZ"),
        ("REQUEST_POINT_KEY", "VARCHAR"),
        ("REQUEST_POINT_LAT", "FLOAT"),
        ("REQUEST_POINT_LON", "FLOAT"),
        ("TOMTOM_ZOOM", "NUMBER"),
        ("TOMTOM_VERSION", "VARCHAR"),
        ("REGION_CODE", "VARCHAR"),
        ("REGION", "VARCHAR"),
        ("H3_R7", "VARCHAR"),
        ("DEGURBA", "NUMBER"),
        ("MACRO_SCORE", "FLOAT"),
        ("TRAFFIC_SCOPE", "VARCHAR"),
        ("NEAR_EV_STATION", "BOOLEAN"),
        ("EV_STATION_ID", "VARCHAR"),
        ("ROAD_FEATURE_ID", "VARCHAR"),
        ("ROAD_OSM_ID", "VARCHAR"),
        ("HIGHWAY", "VARCHAR"),
        ("MAXSPEED_KPH", "FLOAT"),
        ("LANES", "FLOAT"),
        ("ROAD_LEN_M", "FLOAT"),
        ("ROAD_CENTROID_WKT_4326", "VARCHAR"),
        ("FRC", "VARCHAR"),
        ("CURRENT_SPEED", "NUMBER"),
        ("FREE_FLOW_SPEED", "NUMBER"),
        ("CURRENT_TRAVEL_TIME", "NUMBER"),
        ("FREE_FLOW_TRAVEL_TIME", "NUMBER"),
        ("CONFIDENCE", "FLOAT"),
        ("ROAD_CLOSURE", "BOOLEAN"),
        ("SEGMENT_LINESTRING_WKT_4326", "VARCHAR"),
        ("SEGMENT_COORDS_JSON", "VARIANT"),
        ("SPEED_RATIO", "FLOAT"),
        ("DELAY_SEC", "NUMBER"),
        ("DELAY_RATIO", "FLOAT"),
        ("HTTP_STATUS", "NUMBER"),
        ("ERROR_MESSAGE", "VARCHAR"),
        ("RAW_JSON", "VARIANT"),
        ("LOAD_TS", "TIMESTAMP_NTZ"),
    ]

    with ctx.cursor() as cur:
        for col, dtype in required_cols:
            cur.execute(f"ALTER TABLE {SF_FACT} ADD COLUMN IF NOT EXISTS {col} {dtype}")

    # 6) Post-fix clamp in Snowflake (prevents UI 'Invalid date' & keeps range sane)
    def _sf_clamp_ts(table_fqn: str, col: str) -> None:
        sql = f"""
        UPDATE {table_fqn}
        SET {col} = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHERE {col} IS NULL
           OR {col} < '2000-01-01'::TIMESTAMP_NTZ
           OR {col} > DATEADD(day, 1, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
        """.strip()
        with ctx.cursor() as _c:
            _c.execute(sql)

    try:
        if len(fact_df) > 0:
            # keep RAW_JSON as dict/list for VARIANT; leave strings as-is
            if "RAW_JSON" in fact_df.columns:
                fact_df["RAW_JSON"] = fact_df["RAW_JSON"].apply(
                    lambda x: x if isinstance(x, (dict, list)) or x is None else x
                )

            write_pandas(
                ctx,
                fact_df,
                table_name=SF_FACT.split(".")[-1],
                schema=SF_SCHEMA,
                database=SF_DB,
            )
            context["ti"].xcom_push(key="sf_rows_loaded", value=int(len(fact_df)))

        _sf_clamp_ts(SF_FACT, "SNAPSHOT_TS")
        _sf_clamp_ts(SF_FACT, "LOAD_TS")

    finally:
        ctx.close()


# ----------------------------
# Databricks DBFS upload (small files)
# ----------------------------
def _dbx_base_url(host: str) -> str:
    h = (host or "").strip()
    if h.startswith("http://") or h.startswith("https://"):
        return h.rstrip("/")
    return f"https://{h}".rstrip("/")


def _dbfs_put(base_url: str, token: str, local_path: str, dbfs_path: str, overwrite: bool = True) -> None:
    """Upload a file to DBFS.

    We use the chunked create/add-block/close flow to avoid HTTP 400 errors
    that can happen with /dbfs/put (contents) due to request-size limits.

    DBFS REST expects paths like "/tmp/..." (no "dbfs:/" prefix).
    """

    def _normalize_dbfs_path(p: str) -> str:
        api_path = p
        if api_path.startswith("dbfs:/"):
            api_path = "/" + api_path[len("dbfs:/"):]
        elif api_path.startswith("dbfs:"):
            api_path = "/" + api_path[len("dbfs:"):].lstrip("/")
        if not api_path.startswith("/"):
            api_path = "/" + api_path
        return api_path

    api_path = _normalize_dbfs_path(dbfs_path)

    headers = {"Authorization": f"Bearer {token}"}

    # 1) create
    create_url = f"{base_url}/api/2.0/dbfs/create"
    create_payload = {"path": api_path, "overwrite": overwrite}
    r = requests.post(create_url, headers=headers, json=create_payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    handle = r.json().get("handle")
    if handle is None:
        raise RuntimeError(f"DBFS create did not return handle for path={api_path}: {r.text}")

    # 2) add-block (chunked)
    add_url = f"{base_url}/api/2.0/dbfs/add-block"
    chunk_size = 1024 * 1024  # 1MB chunks

    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            data_b64 = base64.b64encode(chunk).decode("utf-8")
            add_payload = {"handle": handle, "data": data_b64}
            ra = requests.post(add_url, headers=headers, json=add_payload, timeout=REQUEST_TIMEOUT)
            ra.raise_for_status()

    # 3) close
    close_url = f"{base_url}/api/2.0/dbfs/close"
    close_payload = {"handle": handle}
    rc = requests.post(close_url, headers=headers, json=close_payload, timeout=REQUEST_TIMEOUT)
    rc.raise_for_status()


# ----------------------------
# Databricks load (fast path)
# ----------------------------
def load_to_databricks_sql(batch_files: List[str], **context):
    """
    Same fast pattern as EV:
      - build small parquet
      - upload to DBFS
      - INSERT INTO target SELECT ... FROM parquet.`dbfs:/...`
    """
    from airflow.hooks.base import BaseHook

    if not batch_files:
        raise ValueError("No batch files provided to load_to_databricks_sql")

    parts: List[pd.DataFrame] = []
    for p in batch_files:
        if not p:
            continue
        with open(p, "r", encoding="utf-8") as f:
            s = f.read()
        if not s.strip():
            continue
        parts.append(pd.read_json(io.StringIO(s), orient="records"))

    if not parts:
        raise ValueError("All batch files were empty; nothing to load")

    fact_df = pd.concat(parts, ignore_index=True)

    # normalize timestamps
    _normalize_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"))
    _clamp_timestamp_columns(fact_df, cols=("LOAD_TS", "SNAPSHOT_TS"), on_out_of_range="now")

    # Databricks SQL can't insert dicts -> stringify RAW_JSON
    if "RAW_JSON" in fact_df.columns:
        fact_df["RAW_JSON"] = fact_df["RAW_JSON"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )

    # Databricks Delta table expects segment_coords_json as STRING
    if "SEGMENT_COORDS_JSON" in fact_df.columns:
        fact_df["SEGMENT_COORDS_JSON"] = fact_df["SEGMENT_COORDS_JSON"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )

    # Lowercase columns to avoid Delta merge conflicts
    fact_df.columns = [str(c).lower() for c in fact_df.columns]

    # Defensive: ensure timestamp columns are plain strings before parquet write
    for c in ("load_ts", "snapshot_ts"):
        if c in fact_df.columns:
            fact_df[c] = fact_df[c].apply(_coerce_ts_value)
            fact_df[c] = pd.to_datetime(fact_df[c], errors="coerce").dt.tz_localize(None)

    # Ensure expected timestamp columns exist in lower-case form
    for c in ("load_ts", "snapshot_ts"):
        if c not in fact_df.columns and c.upper() in fact_df.columns:
            fact_df[c] = fact_df[c.upper()]

    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    if not http_path:
        raise ValueError("databricks_sql.extra.http_path is required")

    base_url = _dbx_base_url(host)

    run_ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    local_dir = f"/tmp/tomtom_traffic_dbx_load_{run_ts}"
    os.makedirs(local_dir, exist_ok=True)

    # write parquet with timestamps as strings (as in EV)
    def _write_parquet_dbx_compatible(df: pd.DataFrame, path: str) -> None:
        """Write parquet with timestamp columns forced to STRING.

        This prevents pyarrow from inferring Spark/Arrow timestamp structs like
        (seconds_since_epoch=..., nanos=...) which later break TRY_TO_TIMESTAMP.
        """
        if df is None or len(df) == 0:
            return

        # Databricks SQL Warehouse is sensitive to Parquet TIMESTAMP(NANOS).
        # We therefore ensure that *no* datetime-like columns are written as Parquet timestamps.
        # Strategy:
        #  1) For known timestamp columns (load_ts/snapshot_ts): coerce -> clamp -> format as STRING.
        #  2) For any other datetime64/object-datetime columns: stringify as well (defensive).
        now_dt = _now_utc().replace(tzinfo=None)

        # 1) Known timestamp columns
        for c in ("load_ts", "snapshot_ts"):
            if c in df.columns:
                df[c] = df[c].apply(_coerce_ts_value)
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.tz_localize(None)
                df.loc[df[c].isna(), c] = pd.Timestamp(now_dt)
                df[c] = df[c].dt.floor("us")
                df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S.%f").astype("string")

        # 2) Defensive: stringify ANY datetime-like columns to avoid TIMESTAMP(NANOS) in parquet
        for col in list(df.columns):
            # pandas datetime64
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    tmp = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
                    tmp = tmp.dt.floor("us")
                    df[col] = tmp.dt.strftime("%Y-%m-%d %H:%M:%S.%f").astype("string")
                    continue
            except Exception:
                pass

            # object columns that may contain datetime objects
            if df[col].dtype == object:
                try:
                    has_dt = df[col].apply(lambda x: isinstance(x, (datetime, pd.Timestamp))).any()
                except Exception:
                    has_dt = False
                if has_dt:
                    tmp = df[col].apply(_coerce_ts_value)
                    tmp = pd.to_datetime(tmp, errors="coerce").dt.tz_localize(None)
                    tmp = tmp.dt.floor("us")
                    df[col] = tmp.dt.strftime("%Y-%m-%d %H:%M:%S.%f").astype("string")

        # Finally write parquet forcing microsecond timestamps if any slip through.
        # allow_truncated_timestamps avoids failures if something still carries ns precision.
        df.to_parquet(
            path,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

    fact_path = os.path.join(local_dir, "fact_tomtom_traffic_flowsegment_snapshots_r7.parquet")
    if len(fact_df) > 0:
        _write_parquet_dbx_compatible(fact_df, fact_path)

    dbfs_dir = f"dbfs:/tmp/tomtom_traffic_flowsegment_r7/{run_ts}"
    if os.path.isfile(fact_path):
        try:
            _dbfs_put(base_url, token, fact_path, f"{dbfs_dir}/fact.parquet")
        except Exception as e:
            size_bytes = os.path.getsize(fact_path)
            raise RuntimeError(f"DBFS upload failed for {fact_path} ({size_bytes} bytes) -> {dbfs_dir}/fact.parquet: {e}")

    def _describe_table(table_fqn: str) -> List[Tuple[str, str]]:
        """Return ordered list of (column_name_lower, data_type) for a Delta table.

        Databricks `DESCRIBE TABLE` may repeat partition columns in a later section;
        we therefore de-dup by column name preserving first occurrence.
        """
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                rows = cur.fetchall()

        cols: List[Tuple[str, str]] = []
        seen: set[str] = set()

        for r in rows:
            if not r:
                continue
            col = str(r[0] or "").strip()
            dtype = str(r[1] or "").strip()

            if not col:
                continue

            # Skip comment/header rows and stop at partition/info sections
            lcol = col.lower()
            if col.startswith("#"):
                continue
            if lcol.startswith("partition") or lcol in ("# partition information", "partition information"):
                break
            if lcol in ("col_name", "column_name"):
                continue

            col_lc = lcol
            if col_lc in seen:
                continue
            seen.add(col_lc)
            cols.append((col_lc, dtype))

        return cols

    def _describe_parquet(dbfs_file: str):
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(f"SELECT * FROM parquet.`{dbfs_file}` LIMIT 0")
                schema = []
                for d in (cur.description or []):
                    schema.append((str(d[0]).lower(), str(d[1]) if len(d) > 1 else "STRING"))
                if not schema:
                    raise RuntimeError(f"Could not infer schema for parquet: {dbfs_file}")
                return schema

    def _insert_from_parquet(table_fqn: str, dbfs_file: str) -> None:
        target_cols = _describe_table(table_fqn)
        src_schema = _describe_parquet(dbfs_file)
        src_cols = [c for c, _t in src_schema]

        insert_cols_sql = ", ".join([f"`{c}`" for c, _t in target_cols])

        if len({c for c, _t in target_cols}) != len(target_cols):
            dupes = []
            seen2 = set()
            for c, _t in target_cols:
                if c in seen2:
                    dupes.append(c)
                seen2.add(c)
            raise RuntimeError(f"DESCRIBE TABLE returned duplicate columns for {table_fqn}: {sorted(set(dupes))}")

        select_exprs: List[str] = []
        for col_lc, dtype in target_cols:
            dt = (dtype or "").upper()
            if col_lc in src_cols:
                if col_lc in ("load_ts", "snapshot_ts") and ("TIMESTAMP" in dt or "DATE" in dt):
                    # Source may be STRING, or sometimes a struct-like rendering such as
                    # '(seconds_since_epoch=1770..., nanos=...)'. Always CAST to STRING and parse defensively.
                    #  - If it looks like seconds_since_epoch, treat it as epoch-nanos/seconds and convert.
                    #  - Else try regular timestamp parsing.
                    select_exprs.append(
                        "CASE "
                        "WHEN CAST(src.`{c}` AS STRING) LIKE '%seconds_since_epoch%' THEN "
                        "  TO_TIMESTAMP( "
                        "    (CAST(REGEXP_EXTRACT(CAST(src.`{c}` AS STRING), 'seconds_since_epoch\\s*[:=\\s]+([0-9]+)', 1) AS DOUBLE)) / 1e9 "
                        "  ) "
                        "ELSE TRY_TO_TIMESTAMP(CAST(src.`{c}` AS STRING)) "
                        "END AS `{c}`".format(c=col_lc)
                    )
                else:
                    select_exprs.append(f"src.`{col_lc}` AS `{col_lc}`")
            else:
                if "TIMESTAMP" in dt:
                    select_exprs.append(f"CAST(NULL AS TIMESTAMP) AS `{col_lc}`")
                elif dt.startswith("DECIMAL") or dt.startswith("NUMERIC"):
                    select_exprs.append(f"CAST(NULL AS {dtype}) AS `{col_lc}`")
                elif dt in ("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "BOOLEAN", "STRING"):
                    select_exprs.append(f"CAST(NULL AS {dt}) AS `{col_lc}`")
                else:
                    select_exprs.append(f"NULL AS `{col_lc}`")

        select_sql = ",\n  ".join(select_exprs)

        sql = f"""
INSERT INTO {table_fqn} ({insert_cols_sql})
SELECT
  {select_sql}
FROM parquet.`{dbfs_file}` AS src
""".strip()

        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(sql)

    if os.path.isfile(fact_path):
        _insert_from_parquet(DBX_FACT, f"{dbfs_dir}/fact.parquet")
        context["ti"].xcom_push(key="dbx_rows_loaded", value=int(len(fact_df)))

    context["ti"].xcom_push(key="dbfs_load_dir", value=dbfs_dir)


# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="tomtom_traffic_flowsegment_r7",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "geo",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["geo", "tomtom", "traffic", "gold", "r7"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_candidates_dbx",
        python_callable=fetch_candidates_from_databricks_sql,
    )

    t_split = PythonOperator(
        task_id="split_batches",
        python_callable=split_candidates_into_batches,
    )

    # Dynamic task mapping: each mapped task processes a slice of the candidates
    t2 = (
        PythonOperator.partial(
            task_id="tomtom_enrich_batch",
            python_callable=enrich_candidates_with_tomtom_traffic_batch,
            pool="tomtom_api_pool",
        )
        .expand(op_kwargs=XComArg(t_split))
    )

    t3 = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        op_kwargs={"batch_files": XComArg(t2)},
    )

    t3b = PythonOperator(
        task_id="load_to_databricks",
        python_callable=load_to_databricks_sql,
        op_kwargs={"batch_files": XComArg(t2)},
    )

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject=(
            "[SUCCESS] TomTom Traffic flowSegment | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>✅ TomTom Traffic flowSegment succeeded</h2>

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
            "[FAILED] TomTom Traffic flowSegment | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>❌ TomTom Traffic flowSegment failed</h2>

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

    # pipeline
    t1 >> t_split >> t2 >> [t3, t3b]
    [t3, t3b] >> success
    [t1, t_split, t2, t3, t3b] >> failure