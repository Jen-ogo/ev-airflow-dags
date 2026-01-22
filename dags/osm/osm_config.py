# dags/osm/osm_config.py
# CONFIG ONLY (no DAG code here)

from __future__ import annotations

COUNTRIES_REGIONS = [
    ("germany", "nordrhein-westfalen"),
    # ("poland", "malopolskie"),
    ("poland", "mazowieckie"),
    # ("poland", "dolnoslaskie"),
    # ("poland", "pomorskie"),
]

EMAIL_TO = ["coach.tutor1993@gmail.com"]

# Host path that is bind-mounted into the osm-tools container as /data
HOST_DATA_DIR = "/Volumes/T7/airflow-docker/data"

ADLS_CONTAINER = "uc-root"
ADLS_BASE_PATH = "raw/osm"  # raw/osm/<country>/<region>/<dataset>/dt=YYYY-MM-DD/

# -----------------------------
# Geofabrik download URLs (EU)
# -----------------------------
GEOFABRIK_EUROPE_BASE = "https://download.geofabrik.de/europe"

# Region PBF (preferred default)
REGION_PBF_URL_TMPL = GEOFABRIK_EUROPE_BASE + "/{country}/{region}-latest.osm.pbf"

# Country (national) PBF (used only for admin via osmium when needed)
COUNTRY_PBF_URL_TMPL = GEOFABRIK_EUROPE_BASE + "/{country}-latest.osm.pbf"

# Region .poly file (used by osmium extract)
REGION_POLY_URL_TMPL = GEOFABRIK_EUROPE_BASE + "/{country}/{region}.poly"

# -----------------------------
# Dataset source routing
# -----------------------------
DATASET_SOURCE = {
    "admin": "regional",
    "roads": "regional",
    "charging": "regional",
    "poi_points": "regional",
    "poi_polygons": "regional",
    "pt_points": "regional",
    "pt_lines": "regional",
    "buildings_activity": "regional",
}

# Osmium settings for the admin cut
OSMIUM_ADMIN_EXTRACT = {
    "strategy": "complete_ways",
    "cut_dirname": "_cut",
    "cut_prefix": "admin_",  # admin_<region>.osm.pbf
}

# Which datasets to create DAGs for
ENABLED_DATASETS = [
    "admin",
    "roads",
    "charging",
    "poi_points",
    "poi_polygons",
    "pt_points",
    "pt_lines",
    "buildings_activity",
]

# Extraction rules per dataset
DATASET_EXTRACT = {
    "admin": {
        "ogr_layer": "multipolygons",
        "where": "boundary='administrative' AND admin_level IN ('4','6')",
        "nlt": "MULTIPOLYGON",
    },
    "roads": {"ogr_layer": "lines", "where": "highway IS NOT NULL", "nlt": None},
    "charging": {
        "ogr_layer": "points",
        "where": "other_tags LIKE '%charging_station%' OR other_tags LIKE '%charging=>yes%'",
        "nlt": None,
    },
    "poi_points": {"ogr_layer": "points", "where": None, "nlt": None},
    "poi_polygons": {"ogr_layer": "multipolygons", "where": None, "nlt": None},
    "pt_points": {"ogr_layer": "points", "where": None, "nlt": None},
    "pt_lines": {"ogr_layer": "lines", "where": None, "nlt": None},
    "buildings_activity": {"ogr_layer": "multipolygons", "where": None, "nlt": None},
}

# Snowflake projection (NO GEOMETRY, only geom_wkt text)
SNOWFLAKE_COLUMNS = {
    "admin": """
        osm_id,
        name,
        admin_level,
        boundary,
        type,
        other_tags
    """,
    "roads": """
        osm_id,
        name,
        highway,
        barrier,
        man_made,
        railway,
        waterway,
        z_order,
        other_tags
    """,
    "charging": """
        osm_id,
        name,
        ref,
        other_tags
    """,
    "poi_points": """
        osm_id,
        name,
        barrier,
        highway,
        is_in,
        ref,
        man_made,
        place,
        other_tags
    """,
    "poi_polygons": """
        osm_id,
        osm_way_id,
        name,
        type,
        aeroway,
        amenity,
        admin_level,
        barrier,
        boundary,
        building,
        craft,
        historic,
        landuse,
        leisure,
        man_made,
        military,
        natural,
        office,
        place,
        shop,
        sport,
        tourism,
        other_tags
    """,
    "pt_points": """
        osm_id,
        name,
        barrier,
        highway,
        is_in,
        ref,
        man_made,
        place,
        other_tags
    """,
    "pt_lines": """
        osm_id,
        name,
        aerialway,
        highway,
        waterway,
        barrier,
        man_made,
        railway,
        z_order,
        other_tags
    """,
    "buildings_activity": """
        osm_id,
        osm_way_id,
        name,
        type,
        aeroway,
        amenity,
        admin_level,
        barrier,
        boundary,
        building,
        craft,
        historic,
        landuse,
        leisure,
        man_made,
        military,
        natural,
        office,
        place,
        shop,
        sport,
        tourism,
        other_tags
    """,
}
