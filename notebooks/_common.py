# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Shared config and helpers
# MAGIC
# MAGIC Every pipeline notebook starts with `%run ./_common`. Table names, thresholds and metric
# MAGIC definitions live **here and only here**, so changing what "on time" means (for example)
# MAGIC is a one-line edit.
# MAGIC
# MAGIC **Naming conventions used across the project**
# MAGIC
# MAGIC | Pattern | Meaning | Example |
# MAGIC |---|---|---|
# MAGIC | `snake_case`, full words | every column outside bronze | `vehicle_id`, not `vid` |
# MAGIC | `*_at` | timestamp | `polled_at` |
# MAGIC | `*_date` | date (Chicago calendar day) | `local_date` |
# MAGIC | `is_*` | boolean | `is_delayed` |
# MAGIC | `*_min`, `*_ft` | unit is in the name | `arrival_error_min` |
# MAGIC | `*_label` | display text for dashboards | `stop_label` |
# MAGIC | plural count | additive measure | `predictions`, `on_time_predictions` |
# MAGIC | `_*` prefix | pipeline metadata, not business data | `_ingested_at` |
# MAGIC
# MAGIC Bronze keeps the CTA API's own field names (`stpid`, `prdtm`, ...) so it mirrors the source exactly.
# MAGIC Renaming happens once, in silver.

# COMMAND ----------

# DBTITLE 1,Imports
import os

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# COMMAND ----------

# DBTITLE 1,Local time zone
# CTA reports times in Chicago local time. With the session time zone set to Chicago:
#   * CTA strings like "20261108 17:42" parse to the correct instant, and
#   * hour(), to_date() and date_format() return Chicago hours and dates, not UTC.
# Every notebook inherits this through %run, so time-of-day is computed the same way everywhere.
LOCAL_TZ = "America/Chicago"
spark.conf.set("spark.sql.session.timeZone", LOCAL_TZ)

# COMMAND ----------

# DBTITLE 1,Catalog, schemas, tables, volumes
CATALOG = "cta_data"

SCHEMA_CONFIG = f"{CATALOG}.config"
SCHEMA_BRONZE = f"{CATALOG}.bronze"
SCHEMA_SILVER = f"{CATALOG}.silver"
SCHEMA_GOLD = f"{CATALOG}.gold"
SCHEMA_OPS = f"{CATALOG}.ops"

# Config
TBL_DISCOVERY_RULES = f"{SCHEMA_CONFIG}.stop_discovery_rules"

# Bronze (raw, CTA field names)
TBL_BRONZE_PREDICTIONS = f"{SCHEMA_BRONZE}.bus_predictions_raw"
TBL_LEGACY_BRONZE = f"{SCHEMA_BRONZE}.bus_predictions"  # pre-refactor table, source for the one-time backfill

# Silver (clean, renamed, one row per real-world thing)
TBL_ROUTES = f"{SCHEMA_SILVER}.routes"
TBL_STOPS = f"{SCHEMA_SILVER}.stops"
TBL_PREDICTIONS = f"{SCHEMA_SILVER}.bus_predictions"
TBL_ARRIVALS = f"{SCHEMA_SILVER}.bus_arrivals"
TBL_OUTCOMES = f"{SCHEMA_SILVER}.prediction_outcomes"

# Gold (dashboard-ready, additive measures)
TBL_GOLD_ACCURACY = f"{SCHEMA_GOLD}.prediction_accuracy"
TBL_GOLD_ERROR_DIST = f"{SCHEMA_GOLD}.prediction_error_distribution"
TBL_GOLD_ARRIVAL_QUALITY = f"{SCHEMA_GOLD}.arrival_quality"
TBL_GOLD_RIDER = f"{SCHEMA_GOLD}.rider_predictions"

# Ops
TBL_INGESTION_RUNS = f"{SCHEMA_OPS}.ingestion_runs"

# Volumes: raw API responses land as files, Auto Loader picks them up
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing/bus_predictions"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/ops/checkpoints"
CHECKPOINT_BRONZE = f"{CHECKPOINT_ROOT}/bronze_bus_predictions_raw"
CHECKPOINT_SILVER_PREDICTIONS = f"{CHECKPOINT_ROOT}/silver_bus_predictions"

# COMMAND ----------

# DBTITLE 1,Pipeline settings and metric definitions
# --- Polling -------------------------------------------------------------------------------
CTA_API_BASE = "http://www.ctabustracker.com/bustime/api/v2"
SECRET_SCOPE, SECRET_KEY = "cta", "api_key"
MAX_STOPS_PER_CALL = 10  # getpredictions accepts at most 10 stop IDs per request

# --- Arrival detection ---------------------------------------------------------------------
# A bus "arrived" when it drops out of a stop's predictions between two polls. We only trust that
# when the next poll came soon enough to bracket the arrival, and the bus was close on its last sighting.
MAX_NEXT_POLL_GAP_SEC = 180       # next poll must follow within 3 min (works for 1- or 2-min polling)
ARRIVAL_MAX_DISTANCE_FT = 1500    # last sighting within ~1-2 min of downtown bus travel
VISIT_SPLIT_GAP_MIN = 15          # fallback: split a vehicle's visits to a stop if unseen this long

# --- Accuracy metrics ----------------------------------------------------------------------
# arrival_error_min = actual arrival - predicted arrival (positive = bus came later than the tracker said)
ON_TIME_EARLY_TOLERANCE_MIN = 3.0  # more than 3 min early counts as "early" (riders can miss it)
ON_TIME_LATE_TOLERANCE_MIN = 3.0   # more than 3 min late counts as "late"
MAX_PREDICTED_WAIT_MIN = 30        # ignore predictions further out than this
ERROR_BIN_MIN, ERROR_BIN_MAX = -10, 15  # histogram range; values outside are clipped to the ends

# --- Incremental processing ----------------------------------------------------------------
REPROCESS_DAYS = 2          # silver/gold rebuild the latest N local dates each run (idempotent)
LOOKBACK_BUFFER_HOURS = 2   # extra history read before the window so visits are not cut in half

# --- Display labels (zero-padded so they sort correctly in dashboards) ---------------------
WAIT_BUCKETS = [
    (0, 2, "00-02 min"),
    (2, 5, "02-05 min"),
    (5, 10, "05-10 min"),
    (10, 15, "10-15 min"),
    (15, 20, "15-20 min"),
    (20, 30, "20-30 min"),
]
# Predictions shown on the rider dashboard: made 2-15 min before the bus arrived
# (1-minute predictions are almost always right and would make every stop look good)
RIDER_WAIT_BUCKETS = ["02-05 min", "05-10 min", "10-15 min"]

TIME_PERIODS = [
    (0, 5, "00-05 Overnight"),
    (5, 7, "05-07 Early morning"),
    (7, 10, "07-10 AM rush"),
    (10, 15, "10-15 Midday"),
    (15, 19, "15-19 PM rush"),
    (19, 24, "19-24 Evening"),
]
DIRECTION_ABBR = {"Northbound": "NB", "Southbound": "SB", "Eastbound": "EB", "Westbound": "WB"}


# Trains are predicted more tightly than buses, so they get a tighter on-time window.
TRAIN_ON_TIME_EARLY_TOLERANCE_MIN = 2.0
TRAIN_ON_TIME_LATE_TOLERANCE_MIN = 2.0


# COMMAND ----------

# DBTITLE 1,Bronze landing schema
# Every CTA field is kept as a string in bronze: no type coercion, no defaults, nothing lost.
# Fields CTA adds later land in _rescued_data instead of being dropped.
CTA_PREDICTION_FIELDS = [
    "tmstmp", "typ", "stpnm", "stpid", "vid", "dstp", "rt", "rtdd", "rtdir",
    "des", "prdtm", "tablockid", "tatripid", "dly", "prdctdn", "zone",
]
CTA_PREDICTION_SCHEMA = StructType([StructField(c, StringType()) for c in CTA_PREDICTION_FIELDS])
LANDING_SCHEMA = StructType(
    CTA_PREDICTION_SCHEMA.fields
    + [StructField("_poll_id", StringType()), StructField("_polled_at_ms", LongType())]
)

# COMMAND ----------

# DBTITLE 1,Column helpers
def clean_str(col_name):
    """Trimmed string, with empty strings turned into NULL."""
    return F.expr(f"nullif(trim({col_name}), '')")


def parse_cta_time(col_name):
    """CTA local time string ("yyyyMMdd HH:mm" or with seconds) -> timestamp. Bad values -> NULL."""
    return F.coalesce(
        F.expr(f"try_to_timestamp({col_name}, 'yyyyMMdd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col_name}, 'yyyyMMdd HH:mm')"),
    )


def minutes_between(later_col, earlier_col):
    """(later - earlier) in fractional minutes."""
    return F.expr(f"(unix_micros({later_col}) - unix_micros({earlier_col})) / 6.0E7")


def bucket_label(value, buckets):
    """Map a numeric column onto [(low, high, label), ...] using low <= value < high."""
    label = None
    for low, high, text in buckets:
        cond = (value >= low) & (value < high)
        label = F.when(cond, text) if label is None else label.when(cond, text)
    return label


def add_time_features(df, ts_col):
    """Chicago-local calendar fields derived from ts_col (session time zone is America/Chicago)."""
    hour = F.hour(ts_col)
    dow = F.dayofweek(ts_col)  # 1 = Sunday ... 7 = Saturday
    monday_first = ((dow + 5) % 7) + 1  # 1 = Monday ... 7 = Sunday
    return (
        df.withColumn("local_date", F.to_date(ts_col))
        .withColumn("hour_of_day", hour)
        .withColumn("hour_label", F.format_string("%02d:00", hour))
        .withColumn("day_of_week", F.concat_ws(" ", monday_first.cast("string"), F.date_format(ts_col, "EEE")))
        .withColumn("day_type", F.when(dow.isin(1, 7), "Weekend").otherwise("Weekday"))
        .withColumn("time_period", bucket_label(hour, TIME_PERIODS))
    )


def direction_abbr(col_name):
    """Northbound -> NB, etc. Unknown values pass through unchanged."""
    expr = F.col(col_name)
    for full, short in DIRECTION_ABBR.items():
        expr = F.when(F.col(col_name) == full, short).otherwise(expr)
    return expr


def rows_processed(query):
    """Total input rows across a finished streaming query's progress reports.

    On serverless (Spark Connect) a progress entry can be a dict or an object, and
    numInputRows can be None for batches that found no new data.
    """
    total = 0
    for p in query.recentProgress:
        n = p.get("numInputRows") if isinstance(p, dict) else getattr(p, "numInputRows", None)
        total += n or 0
    return total


def chunked(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]

# COMMAND ----------

# DBTITLE 1,Incremental write helpers
from datetime import date, timedelta


def reprocess_start_date(source_table, full_refresh):
    """First local_date to rebuild: all history on a full refresh, otherwise the latest REPROCESS_DAYS dates."""
    if full_refresh:
        return date(1900, 1, 1)
    latest = spark.table(source_table).agg(F.max("local_date")).first()[0]
    return None if latest is None else latest - timedelta(days=REPROCESS_DAYS - 1)


def overwrite_dates(df, table, start_date):
    """Atomically replace every row with local_date >= start_date (Delta replaceWhere). Re-runs are idempotent."""
    (
        df.filter(F.col("local_date") >= F.lit(start_date))
        .write.mode("overwrite")
        .option("replaceWhere", f"local_date >= DATE'{start_date.isoformat()}'")
        .saveAsTable(table)
    )

# COMMAND ----------

# DBTITLE 1,CTA API client
_cta_session = None


def get_cta_api_key():
    """API key from the Databricks secret scope, falling back to the CTA_API_KEY env var."""
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    except Exception:
        key = os.environ.get("CTA_API_KEY", "")
        if not key:
            raise RuntimeError(
                f"No CTA API key found. Store it once with: databricks secrets put-secret {SECRET_SCOPE} {SECRET_KEY}"
            ) from None
        return key


def call_cta(endpoint, **params):
    """GET a Bus Tracker endpoint and return its 'bustime-response' body (retries 429/5xx)."""
    global _cta_session
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    if _cta_session is None:
        retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.params = {"key": get_cta_api_key(), "format": "json"}
        _cta_session = session

    try:
        resp = _cta_session.get(f"{CTA_API_BASE}/{endpoint}", params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("bustime-response", {})
    except requests.RequestException as e:
        # Re-raised without the request URL so the API key never ends up in job logs.
        status = getattr(getattr(e, "response", None), "status_code", None)
        detail = f", HTTP {status}" if status else ""
        raise RuntimeError(f"CTA {endpoint} request failed ({type(e).__name__}{detail})") from None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Tracker ('L')
# MAGIC
# MAGIC Everything below is used only by the `train_*` notebooks. The train pipeline reuses the bus thresholds
# MAGIC above (on-time tolerances, wait buckets, time periods, polling-gap limit), so both modes are scored
# MAGIC the same way and can sit side by side in the dashboard.
# MAGIC
# MAGIC Where trains differ from buses:
# MAGIC
# MAGIC | | Bus | Train |
# MAGIC |---|---|---|
# MAGIC | Polled by | stop (10 per call) | station (4 per call); one station returns all its platforms |
# MAGIC | `stop_id` | bus stop pole | platform (`stpId`), one per direction |
# MAGIC | Vehicle key | `vid` + `tatripid` | run number (`rn`) + route; no trip ID |
# MAGIC | "Close" on last sighting | within 1,500 ft or DUE | Approaching/Due flag, or predicted within a few minutes |
# MAGIC | Extra flag | | `isSch`: prediction came from the timetable, not a live train |

# COMMAND ----------

# DBTITLE 1,Train: tables and volumes
TBL_TRAIN_DISCOVERY_RULES = f"{SCHEMA_CONFIG}.train_stop_discovery_rules"

TBL_TRAIN_BRONZE_PREDICTIONS = f"{SCHEMA_BRONZE}.train_predictions_raw"

TBL_TRAIN_ROUTES = f"{SCHEMA_SILVER}.train_routes"
TBL_TRAIN_STOPS = f"{SCHEMA_SILVER}.train_stops"
TBL_TRAIN_PREDICTIONS = f"{SCHEMA_SILVER}.train_predictions"
TBL_TRAIN_ARRIVALS = f"{SCHEMA_SILVER}.train_arrivals"
TBL_TRAIN_OUTCOMES = f"{SCHEMA_SILVER}.train_prediction_outcomes"

TBL_TRAIN_GOLD_ACCURACY = f"{SCHEMA_GOLD}.train_prediction_accuracy"
TBL_TRAIN_GOLD_ERROR_DIST = f"{SCHEMA_GOLD}.train_prediction_error_distribution"
TBL_TRAIN_GOLD_ARRIVAL_QUALITY = f"{SCHEMA_GOLD}.train_arrival_quality"
TBL_TRAIN_GOLD_RIDER = f"{SCHEMA_GOLD}.train_rider_predictions"

TBL_TRAIN_INGESTION_RUNS = f"{SCHEMA_OPS}.train_ingestion_runs"

TRAIN_LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing/train_predictions"
CHECKPOINT_TRAIN_BRONZE = f"{CHECKPOINT_ROOT}/bronze_train_predictions_raw"
CHECKPOINT_TRAIN_SILVER_PREDICTIONS = f"{CHECKPOINT_ROOT}/silver_train_predictions"

# COMMAND ----------

# DBTITLE 1,Train: pipeline settings
# --- Polling -------------------------------------------------------------------------------
TRAIN_API_BASE = "http://lapi.transitchicago.com/api/1.0"
TRAIN_SECRET_KEY = "train_api_key"  # same secret scope as the bus key; Train Tracker uses its own key
MAX_STATIONS_PER_CALL = 4           # ttarrivals rejects more than 4 mapids per request (error 105)
TRAIN_DAILY_CALL_LIMIT = 100000     # CTA default cap per key per day

# Station and platform list: the CTA "List of 'L' Stops" dataset on the Chicago Data Portal (no key needed).
TRAIN_STOPS_URL = "https://data.cityofchicago.org/resource/8pix-ypme.json"

# --- Arrival detection ---------------------------------------------------------------------
# Trains report no distance, so "close on its last sighting" means: Train Tracker was showing
# Approaching/Due, or the last prediction put the train within this many minutes of the platform.
TRAIN_ARRIVAL_MAX_LAST_WAIT_MIN = 3.0
# Predictions flagged isSch come from the timetable, not a live train. They are kept in
# silver.train_predictions but left out of arrivals and scoring unless this is True.
TRAIN_SCORE_SCHEDULED = False

# --- Lines ---------------------------------------------------------------------------------
# (route_id used by the API, name, CTA color, flag columns in the stops dataset)
TRAIN_LINES = [
    ("Red", "Red Line", "c60c30", ["red"]),
    ("Blue", "Blue Line", "00a1de", ["blue"]),
    ("Brn", "Brown Line", "62361b", ["brn"]),
    ("G", "Green Line", "009b3a", ["g"]),
    ("Org", "Orange Line", "f9461c", ["o"]),
    ("P", "Purple Line", "522398", ["p", "pexp"]),
    ("Pink", "Pink Line", "e27ea6", ["pnk"]),
    ("Y", "Yellow Line", "f9e300", ["y"]),
]
TRAIN_DIRECTION_NAMES = {"N": "Northbound", "S": "Southbound", "E": "Eastbound", "W": "Westbound"}

# COMMAND ----------

# DBTITLE 1,Train: bronze landing schema
# Same rule as bus bronze: CTA's own field names, every value a string.
TRAIN_PREDICTION_FIELDS = [
    "staId", "stpId", "staNm", "stpDe", "rn", "rt", "destSt", "destNm", "trDr",
    "prdt", "arrT", "isApp", "isSch", "isDly", "isFlt", "flags", "lat", "lon", "heading",
]
TRAIN_PREDICTION_SCHEMA = StructType([StructField(c, StringType()) for c in TRAIN_PREDICTION_FIELDS])
TRAIN_LANDING_SCHEMA = StructType(
    TRAIN_PREDICTION_SCHEMA.fields
    + [StructField("_poll_id", StringType()), StructField("_polled_at_ms", LongType())]
)

# COMMAND ----------

# DBTITLE 1,Train: column helpers
def parse_train_time(col_name):
    """Train Tracker local time string -> timestamp. Bad values -> NULL.

    JSON responses use "2026-11-11T17:42:05"; the XML format ("20261111 17:42:05") is accepted too.
    """
    return F.coalesce(
        F.try_to_timestamp(F.col(col_name), F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        parse_cta_time(col_name),
    )


def train_flag(col_name):
    """Train Tracker "1"/"0" flag -> boolean (missing = false)."""
    return F.coalesce(clean_str(col_name) == "1", F.lit(False))

# COMMAND ----------

# DBTITLE 1,Train: API client
_cta_train_session = None


def get_cta_train_api_key():
    """Train Tracker key from the Databricks secret scope, falling back to the CTA_TRAIN_API_KEY env var."""
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=TRAIN_SECRET_KEY)
    except Exception:
        key = os.environ.get("CTA_TRAIN_API_KEY", "")
        if not key:
            raise RuntimeError(
                "No CTA Train Tracker key found. Store it once with: "
                f"databricks secrets put-secret {SECRET_SCOPE} {TRAIN_SECRET_KEY}"
            ) from None
        return key


def call_cta_train(endpoint, **params):
    """GET a Train Tracker endpoint and return its 'ctatt' body (retries 429/5xx).

    A list value is sent as a repeated parameter, e.g. mapid=[40380, 40260] -> mapid=40380&mapid=40260.
    Train Tracker reports problems inside the body: check errCd ("0" = OK) and errNm.
    """
    global _cta_train_session
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    if _cta_train_session is None:
        retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.params = {"key": get_cta_train_api_key(), "outputType": "JSON"}
        _cta_train_session = session

    try:
        resp = _cta_train_session.get(f"{TRAIN_API_BASE}/{endpoint}", params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("ctatt", {})
    except (requests.RequestException, ValueError) as e:
        # Re-raised without the request URL so the API key never ends up in job logs.
        status = getattr(getattr(e, "response", None), "status_code", None)
        detail = f", HTTP {status}" if status else ""
        raise RuntimeError(f"CTA train {endpoint} request failed ({type(e).__name__}{detail})") from None

# COMMAND ----------

# Stations the stops dataset still lists but Train Tracker rejects (e.g. closed for construction)
TRAIN_EXCLUDED_STATIONS = {"40260"}  # State/Lake currently closed