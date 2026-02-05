from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
import io
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from ev_repository.dags.osm.osm_config import EMAIL_TO

# Optional deps:
# pip install databricks-sql-connector snowflake-connector-python pandas pyarrow
import databricks.sql as dbsql
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ----------------------------
# Config
# ----------------------------
# Source candidates (Databricks)
DBX_TABLE_CANDIDATES = "geo_databricks_sub.GOLD.FEAT_H3_MACRO_SCORE_R7"

# Target tables (Snowflake)
SF_DB = "GEO_PROJECT"
SF_SCHEMA = "GOLD"
SF_DIM = f"{SF_DB}.{SF_SCHEMA}.DIM_TOMTOM_EV_STATIONS"
SF_MAP = f"{SF_DB}.{SF_SCHEMA}.MAP_CANDIDATE_TOMTOM_STATIONS"
SF_FACT = f"{SF_DB}.{SF_SCHEMA}.FACT_TOMTOM_EV_AVAILABILITY_SNAPSHOTS"

# Target tables (Databricks)
DBX_CATALOG = "geo_databricks_sub"
DBX_SCHEMA = "GOLD"
DBX_DIM = f"{DBX_CATALOG}.{DBX_SCHEMA}.DIM_TOMTOM_EV_STATIONS"
DBX_MAP = f"{DBX_CATALOG}.{DBX_SCHEMA}.MAP_CANDIDATE_TOMTOM_STATIONS"
DBX_FACT = f"{DBX_CATALOG}.{DBX_SCHEMA}.FACT_TOMTOM_EV_AVAILABILITY_SNAPSHOTS"


# TomTom endpoints
# POI Search API (EV charging stations)
TOMTOM_POI_URL = "https://api.tomtom.com/search/2/poiSearch/EV%20charging%20station.json"

# Charging Availability API (by chargingAvailability id)
TOMTOM_AVAIL_URL = "https://api.tomtom.com/search/2/chargingAvailability.json"

# TomTom API usage tuning
DEFAULT_LIMIT = 10  # POI hits per candidate (keep low for trial quotas)
TOP_CANDIDATES = 50  # total candidates per run (stratified by DEGURBA)
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.10  # trotling between TomTom calls

# Default key (bootstrap) — used only if Airflow Variable is missing.
DEFAULT_TOMTOM_API_KEY = "U5bhisKpXvV8TQAB74OgSaUBe8oECtIV"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)



def _radius_from_kring_area(kring_area_m2: float) -> int:
    # radius = sqrt(area/pi), bounded
    if kring_area_m2 is None or kring_area_m2 <= 0:
        return 3000
    r = int(math.sqrt(float(kring_area_m2) / math.pi))
    return int(max(1500, min(10000, r)))


# ------------------------------------------------------
# Timestamp normalization helpers
# ------------------------------------------------------
def _coerce_ts_value(v: Any) -> Any:
    """Coerce various timestamp representations to naive python datetime.

    Handles:
    - pandas/py datetime objects
    - ISO strings
    - Spark/Arrow-style structs like {'seconds_since_epoch': ..., 'nanos': ...}
    - Stringified structs like "(seconds_since_epoch=..., nanos=...)"
    - Raw epoch numbers (seconds/ms/us/ns) as int/float/str

    NOTE: We defensively clamp insane years later in `_clamp_timestamp_columns`.
    """

    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None

    # pandas.Timestamp / datetime
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)

    def _epoch_to_dt(x: float) -> datetime | None:
        """Interpret epoch-like numeric value x into datetime.

        Heuristic by magnitude:
          - >= 1e18: nanoseconds
          - >= 1e15: microseconds
          - >= 1e12: milliseconds
          - else: seconds
        """
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

    # Dict/struct: {'seconds_since_epoch': 1770..., 'nanos': ...}
    if isinstance(v, dict):
        if "seconds_since_epoch" in v:
            dt = _epoch_to_dt(v.get("seconds_since_epoch"))
            return dt
        # try generic parse
        try:
            dt = pd.to_datetime(v, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    # Raw epoch numbers (int/float)
    if isinstance(v, (int, float)):
        return _epoch_to_dt(v)

    # Stringified struct / ISO / epoch strings
    if isinstance(v, str):
        s = v.strip()

        # Pure numeric string => treat as epoch
        if s and all(ch.isdigit() or ch in "+-." for ch in s) and any(ch.isdigit() for ch in s):
            try:
                return _epoch_to_dt(float(s))
            except Exception:
                return None

        if "seconds_since_epoch" in s:
            import re

            m = re.search(r"seconds_since_epoch=([0-9]+)", s)
            if m:
                return _epoch_to_dt(float(m.group(1)))

        # ISO / other timestamp strings
        try:
            dt = pd.to_datetime(s, errors="coerce", utc=True)
            if pd.isna(dt):
                return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    return None


def _normalize_timestamp_columns(df: pd.DataFrame, cols: Tuple[str, ...] = ("UPDATED_AT", "LOAD_TS", "SNAPSHOT_TS")) -> None:
    """In-place normalize timestamp columns to pandas datetime64[ns] (naive)."""
    if df is None or len(df) == 0:
        return
    for c in cols:
        if c in df.columns:
            # First coerce weird structs/strings -> python datetime
            df[c] = df[c].apply(_coerce_ts_value)
            # Then normalize to pandas datetime64
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.tz_localize(None)


# ------------------------------------------------------
# Timestamp clamping helper
# ------------------------------------------------------
def _clamp_timestamp_columns(
    df: pd.DataFrame,
    cols: Tuple[str, ...] = ("UPDATED_AT", "LOAD_TS", "SNAPSHOT_TS"),
    min_ts: str = "2000-01-01",
    max_ts: str = "2100-01-01",
    on_out_of_range: str = "null",
) -> None:
    """In-place clamp timestamp columns to a sane range.

    This fixes cases like year 58066 which can appear if epoch values were misinterpreted
    somewhere in the pipeline.

    on_out_of_range:
      - 'null'  => set to NULL
      - 'now'   => set to current load timestamp
    """
    if df is None or len(df) == 0:
        return

    min_dt = pd.Timestamp(min_ts)
    max_dt = pd.Timestamp(max_ts)
    now_dt = pd.Timestamp(_now_utc().replace(tzinfo=None))

    for c in cols:
        if c not in df.columns:
            continue
        # ensure datetime64
        df[c] = pd.to_datetime(df[c], errors="coerce")
        mask_bad = df[c].isna() | (df[c] < min_dt) | (df[c] > max_dt)
        if mask_bad.any():
            if on_out_of_range == "now":
                df.loc[mask_bad, c] = now_dt
            else:
                df.loc[mask_bad, c] = pd.NaT


# ----------------------------
# Databricks: fetch candidates
# ----------------------------
def fetch_candidates_from_databricks_sql(**context):
    """
    Reads candidates from Databricks SQL Warehouse and pushes a compact list to XCom.
    Requires Airflow Connection: databricks_sql (server_hostname/http_path/token).
    """
    from airflow.hooks.base import BaseHook

    # NOTE: must match docker-compose env var AIRFLOW_CONN_DATABRICKS_SQL
    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password  # store PAT token in "password"
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
        -- WKT: "POINT(lon lat)"; parse without regexp to avoid pattern/escape issues
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

    # add radius
    df["radius_m"] = df["kring_area_m2"].apply(_radius_from_kring_area).astype(int)

    # Keep only what we need in XCom (list of dicts)
    payload = df[["region_code", "region", "h3_r7", "degurba", "lat", "lon", "radius_m"]].to_dict("records")
    context["ti"].xcom_push(key="candidates", value=payload)


# ----------------------------
# TomTom calls
# ----------------------------
def _requests_get_json(url: str, params: Dict[str, Any], max_retries: int = 5) -> Dict[str, Any]:
    last_err = None
    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.5 * (2 ** i))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"GET failed after retries: {url} params={params} err={last_err}")


def enrich_candidates_with_tomtom(**context):
    # TomTom API key: prefer Airflow Variable; if missing — bootstrap from DEFAULT_TOMTOM_API_KEY and persist it.
    tomtom_key = Variable.get("TOMTOM_API_KEY", default_var=None)

    if not tomtom_key:
        # If the variable doesn't exist yet (or is empty), we set it once.
        tomtom_key = DEFAULT_TOMTOM_API_KEY
        try:
            Variable.set("TOMTOM_API_KEY", tomtom_key)
        except Exception:
            # If DB is not writable for some reason, continue with the in-code default.
            pass

    if not tomtom_key:
        raise KeyError("TomTom API key not found")
    candidates: List[Dict[str, Any]] = context["ti"].xcom_pull(key="candidates", task_ids="fetch_candidates_dbx")
    if not candidates:
        raise ValueError("No candidates pulled from Databricks")

    dim_rows = []
    map_rows = []
    fact_rows = []
    # Single run timestamp in UTC (stored as naive)
    load_ts = _now_utc().replace(tzinfo=None)

    for cand in candidates:
        lat = float(cand["lat"])
        lon = float(cand["lon"])
        radius_m = int(cand["radius_m"])

        # 1) POI search
        search_params = {
            "key": tomtom_key,
            "lat": lat,
            "lon": lon,
            "radius": radius_m,
            "limit": DEFAULT_LIMIT,
        }
        search_json = _requests_get_json(TOMTOM_POI_URL, search_params)
        results = search_json.get("results", []) or []

        # build MAP rows
        # rank by dist
        results_sorted = sorted(results, key=lambda x: x.get("dist", 10**18))
        for rank, item in enumerate(results_sorted, start=1):
            poi_id = item.get("id")
            dist = item.get("dist")
            score = item.get("score")

            map_rows.append({
                "REGION_CODE": cand["region_code"],
                "REGION": cand["region"],
                "H3_R7": cand["h3_r7"],
                "DEGURBA": int(cand.get("degurba")) if cand.get("degurba") is not None else None,
                "QUERY_LAT": lat,
                "QUERY_LON": lon,
                "RADIUS_M": radius_m,
                "TOMTOM_POI_ID": poi_id,
                "DIST_M": float(dist) if dist is not None else None,
                "SCORE": float(score) if score is not None else None,
                "RANK_BY_DIST": rank,
                "LOAD_TS": load_ts,
                "RAW_SEARCH_JSON": search_json,  # Snowflake VARIANT supports dict
            })

            # 2) DIM station + availability id
            poi = item.get("poi", {}) or {}
            addr = item.get("address", {}) or {}
            pos = item.get("position", {}) or {}
            charging_av_id = (((item.get("dataSources") or {}).get("chargingAvailability") or {}).get("id"))

            brand = None
            brands = poi.get("brands") or []
            if brands and isinstance(brands, list):
                brand = (brands[0] or {}).get("name")

            connectors_static = (item.get("chargingPark") or {}).get("connectors")

            dim_rows.append({
                "TOMTOM_POI_ID": poi_id,
                "CHARGING_AVAILABILITY_ID": charging_av_id,
                "NAME": poi.get("name"),
                "BRAND": brand,
                "CATEGORY": (poi.get("categories") or [None])[0],
                "COUNTRY_CODE": addr.get("countryCode"),
                "COUNTRY_SUBDIVISION": addr.get("countrySubdivisionName"),
                "MUNICIPALITY": addr.get("municipality"),
                "STREET": addr.get("streetName"),
                "STREET_NUMBER": addr.get("streetNumber"),
                "POSTAL_CODE": addr.get("postalCode"),
                "FREEFORM_ADDRESS": addr.get("freeformAddress"),
                "LAT": pos.get("lat"),
                "LON": pos.get("lon"),
                "CONNECTORS_STATIC_JSON": json.dumps(connectors_static, ensure_ascii=False) if connectors_static is not None else None,
                "RAW_POI_JSON": item,  # Snowflake VARIANT supports dict
                "UPDATED_AT": load_ts,
            })

            # 3) Availability snapshot (with chargingAvailability id)
            if charging_av_id:
                avail_params = {"key": tomtom_key, "chargingAvailability": charging_av_id}
                avail_json = _requests_get_json(TOMTOM_AVAIL_URL, avail_params)

                snapshot_ts = load_ts
                for c in (avail_json.get("connectors") or []):
                    ctype = c.get("type")
                    total = c.get("total")
                    av = (c.get("availability") or {}).get("current") or {}
                    ppl = (c.get("availability") or {}).get("perPowerLevel") or []

                    # if perPowerLevel present wirte by it, else power_kw = null
                    if ppl:
                        for pl in ppl:
                            fact_rows.append({
                                "CHARGING_AVAILABILITY_ID": charging_av_id,
                                "SNAPSHOT_TS": snapshot_ts,
                                "CONNECTOR_TYPE": ctype,
                                "POWER_KW": pl.get("powerKW"),
                                "TOTAL": total,
                                "AVAILABLE": pl.get("available"),
                                "OCCUPIED": pl.get("occupied"),
                                "RESERVED": pl.get("reserved"),
                                "UNKNOWN": pl.get("unknown"),
                                "OUT_OF_SERVICE": pl.get("outOfService"),
                                "RAW_AVAIL_JSON": avail_json,
                            })
                    else:
                        fact_rows.append({
                            "CHARGING_AVAILABILITY_ID": charging_av_id,
                            "SNAPSHOT_TS": snapshot_ts,
                            "CONNECTOR_TYPE": ctype,
                            "POWER_KW": None,
                            "TOTAL": total,
                            "AVAILABLE": av.get("available"),
                            "OCCUPIED": av.get("occupied"),
                            "RESERVED": av.get("reserved"),
                            "UNKNOWN": av.get("unknown"),
                            "OUT_OF_SERVICE": av.get("outOfService"),
                            "RAW_AVAIL_JSON": avail_json,
                        })

            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    # de-dup DIM by TOMTOM_POI_ID (latest wins)
    dim_df = pd.DataFrame(dim_rows).drop_duplicates(subset=["TOMTOM_POI_ID"], keep="last")
    map_df = pd.DataFrame(map_rows)
    fact_df = pd.DataFrame(fact_rows)

    # Normalize + clamp timestamps BEFORE serialization (prevents insane years like 58066)
    _normalize_timestamp_columns(dim_df)
    _normalize_timestamp_columns(map_df)
    _normalize_timestamp_columns(fact_df)

    _clamp_timestamp_columns(dim_df, on_out_of_range="now")
    _clamp_timestamp_columns(map_df, on_out_of_range="now")
    _clamp_timestamp_columns(fact_df, on_out_of_range="now")

    # Serialize timestamps as plain strings to keep Snowflake/Databricks connectors from misinterpreting epochs
    ts_cols = ("UPDATED_AT", "LOAD_TS", "SNAPSHOT_TS")
    for _df in (dim_df, map_df, fact_df):
        for _c in ts_cols:
            if _c in _df.columns:
                _df[_c] = pd.to_datetime(_df[_c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # Persist to XCom as JSON-serializable chunks
    context["ti"].xcom_push(
        key="dim_df_json",
        value=dim_df.to_json(orient="records", force_ascii=False),
    )
    context["ti"].xcom_push(
        key="map_df_json",
        value=map_df.to_json(orient="records", force_ascii=False),
    )
    context["ti"].xcom_push(
        key="fact_df_json",
        value=fact_df.to_json(orient="records", force_ascii=False),
    )


# ----------------------------
# Snowflake load
# ----------------------------
def load_to_snowflake(**context):
    from airflow.hooks.base import BaseHook

    dim_json = context["ti"].xcom_pull(key="dim_df_json", task_ids="tomtom_enrich")
    map_json = context["ti"].xcom_pull(key="map_df_json", task_ids="tomtom_enrich")
    fact_json = context["ti"].xcom_pull(key="fact_df_json", task_ids="tomtom_enrich")

    dim_df = pd.read_json(io.StringIO(dim_json), orient="records")
    map_df = pd.read_json(io.StringIO(map_json), orient="records")
    fact_df = pd.read_json(io.StringIO(fact_json), orient="records")

    # Normalize timestamps coming from XCom JSON (defensive: handles Spark-style structs too)
    _normalize_timestamp_columns(dim_df)
    _normalize_timestamp_columns(map_df)
    _normalize_timestamp_columns(fact_df)

    # Clamp insane timestamps (e.g., year 58066) to keep Snowflake usable
    _clamp_timestamp_columns(dim_df, on_out_of_range="now")
    _clamp_timestamp_columns(map_df, on_out_of_range="now")
    _clamp_timestamp_columns(fact_df, on_out_of_range="now")

    # NOTE: must match docker-compose env var AIRFLOW_CONN_SF_CONN
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

    # Ensure Snowflake target tables have the columns we are going to load.
    # If a column is missing, write_pandas will fail with "invalid identifier".
    ddl_statements = [
        f"ALTER TABLE {SF_MAP} ADD COLUMN IF NOT EXISTS DEGURBA NUMBER",
    ]

    cur = ctx.cursor()
    try:
        for ddl in ddl_statements:
            cur.execute(ddl)
    finally:
        cur.close()

    def _sf_clamp_ts(table_fqn: str, col: str) -> None:
        """Clamp out-of-range timestamps that show as 'Invalid date' in UI.

        We keep timestamps usable by forcing them into a sane range.
        This is especially important for cases like year 58066 which can appear
        if an epoch-like value was misinterpreted upstream.
        """
        sql = f"""
        UPDATE {table_fqn}
        SET {col} = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHERE {col} IS NULL
           OR {col} < '2000-01-01'::TIMESTAMP_NTZ
           OR {col} > DATEADD(day, 1, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
        """.strip()
        with ctx.cursor() as _c:
            _c.execute(sql)

    def _sf_postfix_clamp_all() -> None:
        # DIM
        _sf_clamp_ts(SF_DIM, "UPDATED_AT")
        # MAP
        _sf_clamp_ts(SF_MAP, "LOAD_TS")
        # FACT
        _sf_clamp_ts(SF_FACT, "SNAPSHOT_TS")

    try:
        # DIM: upsert via write_pandas (append)
        # merge can be via temp stage or via a staging table. We choose staging table for better visibility and easier debugging.
        if len(dim_df) > 0:
            write_pandas(ctx, dim_df, table_name="DIM_TOMTOM_EV_STATIONS", schema=SF_SCHEMA, database=SF_DB)

        if len(map_df) > 0:
            write_pandas(ctx, map_df, table_name="MAP_CANDIDATE_TOMTOM_STATIONS", schema=SF_SCHEMA, database=SF_DB)

        if len(fact_df) > 0:
            write_pandas(ctx, fact_df, table_name="FACT_TOMTOM_EV_AVAILABILITY_SNAPSHOTS", schema=SF_SCHEMA, database=SF_DB)

        # Post-fix clamp for any bad timestamps that can still slip through
        # and render as 'Invalid date' in Snowflake UI.
        _sf_postfix_clamp_all()

    finally:
        ctx.close()


# ----------------------------
# Databricks DBFS upload (fast path for small landing files)
# ----------------------------

def _dbx_base_url(host: str) -> str:
    h = (host or "").strip()
    if h.startswith("http://") or h.startswith("https://"):
        return h.rstrip("/")
    return f"https://{h}".rstrip("/")


def _dbfs_put_small(base_url: str, token: str, local_path: str, dbfs_path: str, overwrite: bool = True) -> None:
    """Upload a small file to DBFS using /api/2.0/dbfs/put.

    NOTE: This uses the simple 'contents' API (base64). Keep files reasonably small.
    Our run is limited (TOP_CANDIDATES=50), so parquets should be small.
    """
    import base64

    size_bytes = os.path.getsize(local_path)
    if size_bytes > 8 * 1024 * 1024:
        raise RuntimeError(
            f"DBFS put (contents) supports small files; got {size_bytes} bytes for {local_path}. "
            "Use a larger-file upload flow (create/add-block/close) or store to ADLS."
        )

    with open(local_path, "rb") as f:
        data_b64 = base64.b64encode(f.read()).decode("utf-8")

    url = f"{base_url}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {token}"}
    # DBFS REST API expects paths like "/tmp/..." (no "dbfs:/" prefix).
    api_path = dbfs_path
    if api_path.startswith("dbfs:/"):
        api_path = "/" + api_path[len("dbfs:/"):]
    elif api_path.startswith("dbfs:"):
        api_path = "/" + api_path[len("dbfs:"):].lstrip("/")
    if not api_path.startswith("/"):
        api_path = "/" + api_path

    payload = {"path": api_path, "contents": data_b64, "overwrite": overwrite}

    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

# ----------------------------
# Databricks load (SQL Warehouse)
# ----------------------------
def load_to_databricks_sql(**context):
    """Fast append into Databricks Delta using COPY INTO from Parquet files uploaded to DBFS.

    Why: `executemany INSERT` via SQL Warehouse is slow. Instead we:
      1) build small Parquet landing files in Airflow
      2) upload them to DBFS via REST
      3) run 3 COPY INTO statements (DIM/MAP/FACT)

    This keeps Databricks load comparable to Snowflake speed for small batches.
    """
    from airflow.hooks.base import BaseHook

    dim_json = context["ti"].xcom_pull(key="dim_df_json", task_ids="tomtom_enrich")
    map_json = context["ti"].xcom_pull(key="map_df_json", task_ids="tomtom_enrich")
    fact_json = context["ti"].xcom_pull(key="fact_df_json", task_ids="tomtom_enrich")

    dim_df = pd.read_json(io.StringIO(dim_json), orient="records")
    map_df = pd.read_json(io.StringIO(map_json), orient="records")
    fact_df = pd.read_json(io.StringIO(fact_json), orient="records")

    # Normalize timestamps from string form
    _normalize_timestamp_columns(dim_df)
    _normalize_timestamp_columns(map_df)
    _normalize_timestamp_columns(fact_df)

    # Clamp insane timestamps (defensive)
    _clamp_timestamp_columns(dim_df, on_out_of_range="now")
    _clamp_timestamp_columns(map_df, on_out_of_range="now")
    _clamp_timestamp_columns(fact_df, on_out_of_range="now")

    # Databricks SQL can't insert dicts — stringify RAW json fields.
    if "RAW_POI_JSON" in dim_df.columns:
        dim_df["RAW_POI_JSON"] = dim_df["RAW_POI_JSON"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )
    if "RAW_SEARCH_JSON" in map_df.columns:
        map_df["RAW_SEARCH_JSON"] = map_df["RAW_SEARCH_JSON"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )
    if "RAW_AVAIL_JSON" in fact_df.columns:
        fact_df["RAW_AVAIL_JSON"] = fact_df["RAW_AVAIL_JSON"].apply(
            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
        )

    # IMPORTANT: Databricks/Delta can treat column names case-sensitively during schema merge.
    # Our DataFrames are in UPPER_SNAKE, while existing Delta tables may have lower_snake.
    # Normalize to lower-case to avoid merge conflicts like 'updated_at' vs 'UPDATED_AT'.
    def _lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return df
        df.columns = [str(c).lower() for c in df.columns]
        return df

    dim_df = _lowercase_columns(dim_df)
    map_df = _lowercase_columns(map_df)
    fact_df = _lowercase_columns(fact_df)

    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    if not http_path:
        raise ValueError("databricks_sql.extra.http_path is required")

    base_url = _dbx_base_url(host)

    run_ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    local_dir = f"/tmp/tomtom_ev_dbx_load_{run_ts}"
    os.makedirs(local_dir, exist_ok=True)

    def _write_parquet_dbx_compatible(df: pd.DataFrame, path: str) -> None:
        """Write Parquet in a Databricks-SQL friendly way.

        We intentionally write *timestamp columns as strings* to avoid:
          - Parquet TIMESTAMP(NANOS) incompatibilities
          - Delta schema merge conflicts when an existing table has a different timestamp type

        We will CAST to the target column types during the INSERT step.
        """
        if df is None or len(df) == 0:
            return

        ts_cols = [c for c in ("updated_at", "load_ts", "snapshot_ts") if c in df.columns]
        for c in ts_cols:
            # Normalize to datetime then format as string with microseconds
            df[c] = pd.to_datetime(df[c], errors="coerce")
            try:
                df[c] = df[c].dt.floor("us")
            except Exception:
                pass
            # Use an unambiguous format; keep as STRING in parquet
            df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Ensure consistent column order for stable ingestion
        df = df.copy()

        df.to_parquet(
            path,
            index=False,
            engine="pyarrow",
        )

    # Write small Parquet landing files
    dim_path = os.path.join(local_dir, "dim_tomtom_ev_stations.parquet")
    map_path = os.path.join(local_dir, "map_candidate_tomtom_stations.parquet")
    fact_path = os.path.join(local_dir, "fact_tomtom_ev_availability_snapshots.parquet")

    if len(dim_df) > 0:
        _write_parquet_dbx_compatible(dim_df, dim_path)
    if len(map_df) > 0:
        _write_parquet_dbx_compatible(map_df, map_path)
    if len(fact_df) > 0:
        _write_parquet_dbx_compatible(fact_df, fact_path)

    # Upload to DBFS
    dbfs_dir = f"dbfs:/tmp/tomtom_ev_enrichment/{run_ts}"

    if os.path.isfile(dim_path):
        _dbfs_put_small(base_url, token, dim_path, f"{dbfs_dir}/dim_tomtom_ev_stations.parquet")
    if os.path.isfile(map_path):
        _dbfs_put_small(base_url, token, map_path, f"{dbfs_dir}/map_candidate_tomtom_stations.parquet")
    if os.path.isfile(fact_path):
        _dbfs_put_small(base_url, token, fact_path, f"{dbfs_dir}/fact_tomtom_ev_availability_snapshots.parquet")

    def _describe_table(table_fqn: str) -> List[Tuple[str, str]]:
        """Return ordered list of (column_name_lower, data_type) for a Delta table."""
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                rows = cur.fetchall()

        cols: List[Tuple[str, str]] = []
        for r in rows:
            if not r:
                continue
            col = str(r[0] or "").strip()
            dtype = str(r[1] or "").strip()
            if not col or col.startswith("#"):
                continue
            # Stop at partition/info sections
            if col.lower() in ("partition", "# partitioning", "# partitions"):
                break
            cols.append((col.lower(), dtype))
        return cols

    def _describe_parquet(dbfs_file: str):
        """Return Parquet schema as list of (col_name, data_type).

        NOTE: Databricks SQL Warehouses may fail on `DESCRIBE TABLE parquet.`...``
        even when `SELECT * FROM parquet.`...`` works. We therefore use
        `SELECT ... LIMIT 0` and rely on cursor.description.
        """
        # Read 0 rows to obtain the schema via DB-API cursor metadata.
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(f"SELECT * FROM parquet.`{dbfs_file}` LIMIT 0")

                schema = []
                # DB-API: description items are typically
                # (name, type_code, display_size, internal_size, precision, scale, null_ok)
                for d in (cur.description or []):
                    col_name = d[0]
                    type_code = d[1] if len(d) > 1 else None

                    # databricks-sql usually returns a string type name; if not, fall back.
                    if isinstance(type_code, str) and type_code:
                        data_type = type_code
                    else:
                        data_type = "STRING"

                    schema.append((col_name, data_type))

                if not schema:
                    raise RuntimeError(
                        f"Could not infer schema for parquet file: {dbfs_file}. "
                        "`SELECT ... LIMIT 0` returned no cursor metadata."
                    )

                return schema

    def _insert_from_parquet(table_fqn: str, dbfs_file: str) -> None:
        """Insert rows from a parquet file into an existing Delta table.

        Key idea: do NOT rely on COPY INTO + mergeSchema (fragile). Instead:
          - read parquet as a relation
          - project exactly the target columns (explicit column list)
          - CAST where needed (especially timestamps)
          - for target columns missing in parquet, insert NULL casted to the target type

        This prevents schema-merge errors like:
          - PARQUET_TYPE_ILLEGAL TIMESTAMP(NANOS)
          - DELTA_FAILED_TO_MERGE_FIELDS (case/type conflicts)
        """
        target_cols = _describe_table(table_fqn)
        if not target_cols:
            raise RuntimeError(f"Failed to DESCRIBE TABLE {table_fqn}")

        src_schema = _describe_parquet(dbfs_file)

        # src_schema is now a list of (col_name, data_type)
        src_cols = [r[0].lower() for r in src_schema]

        insert_cols_sql = ", ".join([f"`{c}`" for c, _t in target_cols])

        select_exprs: List[str] = []
        for col_lc, dtype in target_cols:
            dt = (dtype or "").upper()

            if col_lc in src_cols:
                if col_lc in ("updated_at", "load_ts", "snapshot_ts"):
                    if "TIMESTAMP" in dt or "DATE" in dt:
                        # src is STRING; parse safely
                        select_exprs.append(f"TRY_TO_TIMESTAMP(src.`{col_lc}`) AS `{col_lc}`")
                    else:
                        select_exprs.append(f"src.`{col_lc}` AS `{col_lc}`")
                else:
                    # keep as-is; Spark will try to coerce if needed
                    select_exprs.append(f"src.`{col_lc}` AS `{col_lc}`")
            else:
                # Column missing in parquet: insert NULL casted to target type where possible
                if "TIMESTAMP" in dt:
                    select_exprs.append(f"CAST(NULL AS TIMESTAMP) AS `{col_lc}`")
                elif dt.startswith("DECIMAL") or dt.startswith("NUMERIC"):
                    select_exprs.append(f"CAST(NULL AS {dtype}) AS `{col_lc}`")
                elif dt in ("BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "BOOLEAN", "STRING"):
                    select_exprs.append(f"CAST(NULL AS {dt}) AS `{col_lc}`")
                else:
                    # fallback
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

    # Robust INSERT (append) with type casting
    if os.path.isfile(dim_path):
        _insert_from_parquet(DBX_DIM, f"{dbfs_dir}/dim_tomtom_ev_stations.parquet")
    if os.path.isfile(map_path):
        _insert_from_parquet(DBX_MAP, f"{dbfs_dir}/map_candidate_tomtom_stations.parquet")
    if os.path.isfile(fact_path):
        _insert_from_parquet(DBX_FACT, f"{dbfs_dir}/fact_tomtom_ev_availability_snapshots.parquet")

    # Save DBFS dir for debugging
    context["ti"].xcom_push(key="dbfs_load_dir", value=dbfs_dir)


# ----------------------------
# Optional: Save parquet for Databricks ingestion
# ----------------------------
def save_parquet_for_databricks(**context):
    dim_json = context["ti"].xcom_pull(key="dim_df_json", task_ids="tomtom_enrich")
    map_json = context["ti"].xcom_pull(key="map_df_json", task_ids="tomtom_enrich")
    fact_json = context["ti"].xcom_pull(key="fact_df_json", task_ids="tomtom_enrich")

    dim_df = pd.read_json(io.StringIO(dim_json), orient="records")
    map_df = pd.read_json(io.StringIO(map_json), orient="records")
    fact_df = pd.read_json(io.StringIO(fact_json), orient="records")

    run_ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    out_dir = f"/tmp/tomtom_ev_{run_ts}"
    os.makedirs(out_dir, exist_ok=True)

    # These are landing files; you can upload them to ADLS/DBFS and MERGE in Databricks.
    dim_path = os.path.join(out_dir, "dim_tomtom_ev_stations.parquet")
    map_path = os.path.join(out_dir, "map_candidate_tomtom_stations.parquet")
    fact_path = os.path.join(out_dir, "fact_tomtom_ev_availability_snapshots.parquet")

    dim_df.to_parquet(dim_path, index=False)
    map_df.to_parquet(map_path, index=False)
    fact_df.to_parquet(fact_path, index=False)

    context["ti"].xcom_push(key="dbx_parquet_dir", value=out_dir)


# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="tomtom_ev_enrichment_r7",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "owner": "geo",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["geo", "tomtom", "ev", "gold", "r7"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_candidates_dbx",
        python_callable=fetch_candidates_from_databricks_sql,
    )

    t2 = PythonOperator(
        task_id="tomtom_enrich",
        python_callable=enrich_candidates_with_tomtom,
    )

    t3 = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t3b = PythonOperator(
        task_id="load_to_databricks",
        python_callable=load_to_databricks_sql,
    )

    t4 = PythonOperator(
        task_id="save_parquet_for_databricks",
        python_callable=save_parquet_for_databricks,
    )

    success = EmailOperator(
        task_id="success",
        to=EMAIL_TO,
        subject=(
            "[SUCCESS] TomTom EV enrichment | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>✅ TomTom EV enrichment succeeded</h2>

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
            "[FAILED] TomTom EV enrichment | dag={{ dag.dag_id }} | ds={{ ds }} | run={{ run_id }}"
        ),
        html_content="""
        <h2>❌ TomTom EV enrichment failed</h2>

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

    # Main pipeline
    t1 >> t2 >> [t3, t3b, t4]

    # Success only after all three load branches succeed
    [t3, t3b, t4] >> success

    # Failure if any task fails
    [t1, t2, t3, t3b, t4] >> failure