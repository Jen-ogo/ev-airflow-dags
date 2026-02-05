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
    """
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None

    # pandas.Timestamp / datetime
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)

    # Dict/struct: {'seconds_since_epoch': 1770..., 'nanos': ...}
    if isinstance(v, dict):
        if "seconds_since_epoch" in v:
            try:
                # Some connectors store ns since epoch
                secs = float(v.get("seconds_since_epoch"))
                # Heuristic: if it's too large, assume nanoseconds
                if secs > 10**12:
                    secs = secs / 1e9
                return datetime.fromtimestamp(secs, tz=timezone.utc).replace(tzinfo=None)
            except Exception:
                return None
        # try generic parse
        try:
            return pd.to_datetime(v, errors="coerce").to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    # Stringified struct: "(seconds_since_epoch=1770..., nanos=...)"
    if isinstance(v, str):
        s = v.strip()
        if "seconds_since_epoch" in s:
            import re

            m = re.search(r"seconds_since_epoch=([0-9]+)", s)
            if m:
                try:
                    secs = float(m.group(1))
                    if secs > 10**12:
                        secs = secs / 1e9
                    return datetime.fromtimestamp(secs, tz=timezone.utc).replace(tzinfo=None)
                except Exception:
                    return None
        # ISO / other timestamp strings
        try:
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    return v


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

            # 3) Availability snapshot (если есть chargingAvailability id)
            if charging_av_id:
                avail_params = {"key": tomtom_key, "chargingAvailability": charging_av_id}
                avail_json = _requests_get_json(TOMTOM_AVAIL_URL, avail_params)

                snapshot_ts = load_ts
                for c in (avail_json.get("connectors") or []):
                    ctype = c.get("type")
                    total = c.get("total")
                    av = (c.get("availability") or {}).get("current") or {}
                    ppl = (c.get("availability") or {}).get("perPowerLevel") or []

                    # если perPowerLevel есть — пишем по нему, иначе power_kw = null
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

    # Persist to XCom as JSON-serializable chunks.
    # IMPORTANT: use ISO timestamps to avoid Snowflake timestamp parsing issues via write_pandas.
    context["ti"].xcom_push(
        key="dim_df_json",
        value=dim_df.to_json(orient="records", force_ascii=False, date_format="iso"),
    )
    context["ti"].xcom_push(
        key="map_df_json",
        value=map_df.to_json(orient="records", force_ascii=False, date_format="iso"),
    )
    context["ti"].xcom_push(
        key="fact_df_json",
        value=fact_df.to_json(orient="records", force_ascii=False, date_format="iso"),
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

    try:
        # DIM: upsert-поведение проще делать через stage+merge, но для старта — append.
        # Если нужен MERGE — скажи, дам Snowflake MERGE через temp stage.
        if len(dim_df) > 0:
            write_pandas(ctx, dim_df, table_name="DIM_TOMTOM_EV_STATIONS", schema=SF_SCHEMA, database=SF_DB)

        if len(map_df) > 0:
            write_pandas(ctx, map_df, table_name="MAP_CANDIDATE_TOMTOM_STATIONS", schema=SF_SCHEMA, database=SF_DB)

        if len(fact_df) > 0:
            write_pandas(ctx, fact_df, table_name="FACT_TOMTOM_EV_AVAILABILITY_SNAPSHOTS", schema=SF_SCHEMA, database=SF_DB)

    finally:
        ctx.close()


# ----------------------------
# Databricks load (SQL Warehouse)
# ----------------------------
def load_to_databricks_sql(**context):
    """Append DIM/MAP/FACT into Databricks Delta tables via SQL Warehouse."""
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

    # Databricks SQL connector can't insert dicts — stringify RAW json fields.
    if "RAW_POI_JSON" in dim_df.columns:
        dim_df["RAW_POI_JSON"] = dim_df["RAW_POI_JSON"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
    if "RAW_SEARCH_JSON" in map_df.columns:
        map_df["RAW_SEARCH_JSON"] = map_df["RAW_SEARCH_JSON"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
    if "RAW_AVAIL_JSON" in fact_df.columns:
        fact_df["RAW_AVAIL_JSON"] = fact_df["RAW_AVAIL_JSON"].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

    conn = BaseHook.get_connection("databricks_sql")
    host = conn.host
    token = conn.password
    http_path = conn.extra_dejson.get("http_path")
    if not http_path:
        raise ValueError("databricks_sql.extra.http_path is required")

    def _dbx_sql_type_from_series(col: str, s: pd.Series) -> str:
        """Best-effort mapping pandas dtype -> Databricks SQL type."""
        c = col.upper()
        if c.startswith("RAW_") or c.endswith("_JSON"):
            return "STRING"
        if c.endswith("_TS") or c.endswith("_AT") or c.endswith("_TIME"):
            return "TIMESTAMP"
        # dtype-based fallbacks
        if pd.api.types.is_integer_dtype(s):
            return "INT"
        if pd.api.types.is_float_dtype(s):
            return "DOUBLE"
        if pd.api.types.is_bool_dtype(s):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(s):
            return "TIMESTAMP"
        return "STRING"

    def _ensure_table_columns(table_fqn: str, df: pd.DataFrame):
        """Ensure Databricks Delta table has all columns present in df (adds missing columns)."""
        if df is None or len(df) == 0:
            return

        # Fetch existing columns via DESCRIBE TABLE
        existing_cols: set[str] = set()
        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                for row in cur.fetchall():
                    # DESCRIBE TABLE returns (col_name, data_type, comment) rows; partition info rows may follow
                    col_name = row[0]
                    if not col_name or str(col_name).strip() == "" or str(col_name).lower().startswith("#"):
                        continue
                    # stop when we reach the partition/information section (Databricks uses empty line or '#')
                    if str(col_name).strip().lower() in ("# col_name", "col_name"):
                        continue
                    if str(col_name).strip().lower() in ("# partition information", "# detailed table information"):
                        break
                    existing_cols.add(str(col_name).strip().lower())

        missing_defs: list[str] = []
        for col in df.columns:
            if str(col).strip().lower() in existing_cols:
                continue
            sql_type = _dbx_sql_type_from_series(col, df[col])
            missing_defs.append(f"`{col}` {sql_type}")

        if not missing_defs:
            return

        alter_sql = f"ALTER TABLE {table_fqn} ADD COLUMNS ({', '.join(missing_defs)})"
        # Execute ALTER; if concurrent runs added some cols already, ignore the error.
        try:
            with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
                with c.cursor() as cur:
                    cur.execute(alter_sql)
        except Exception:
            pass

    def _insert_df(table_fqn: str, df: pd.DataFrame):
        if df is None or len(df) == 0:
            return

        # Make sure the destination table schema can accept all columns.
        _ensure_table_columns(table_fqn, df)

        cols = list(df.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join([f"`{c}`" for c in cols])
        sql_ins = f"INSERT INTO {table_fqn} ({col_list}) VALUES ({placeholders})"
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]

        with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as c:
            with c.cursor() as cur:
                cur.executemany(sql_ins, values)

    # Append
    _insert_df(DBX_DIM, dim_df)
    _insert_df(DBX_MAP, map_df)
    _insert_df(DBX_FACT, fact_df)


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