# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Train · Bronze · Ingest CTA train predictions (job: every 1-2 minutes)
# MAGIC
# MAGIC The train twin of `bronze`. One notebook for everything that touches the Train Tracker API:
# MAGIC
# MAGIC 1. **Setup**: creates the train config, bronze, reference and ops tables on the first run.
# MAGIC 2. **Stop discovery** (at most daily): turns `config.train_stop_discovery_rules` (area + bounding box) into
# MAGIC    `silver.train_stops`, using CTA's "List of 'L' Stops" dataset on the Chicago Data Portal.
# MAGIC    To change coverage, edit the rules table and set `rediscover = true` once.
# MAGIC 3. **Poll**: calls `ttarrivals` for every active station, 4 stations per call, and lands each poll as a
# MAGIC    JSON-lines file in the `bronze.landing` volume (`train_predictions/` folder).
# MAGIC 4. **Auto Loader** (`availableNow`): loads new files into `bronze.train_predictions_raw` exactly once.
# MAGIC 5. **Run log**: one row per run in `ops.train_ingestion_runs`.
# MAGIC
# MAGIC **Stations vs stops.** A station (`staId`, 4xxxx) has one platform per direction (`stpId`, 3xxxx). We poll by
# MAGIC station, which returns every platform's predictions in one go; everything downstream is by platform, so
# MAGIC `stop_id` means the same kind of thing as it does for buses: one place where riders wait for one direction.
# MAGIC
# MAGIC **Job settings:** every 1-2 min, max concurrent runs = 1, 1 retry. About 17 Loop stations = 5 calls per poll,
# MAGIC about 7,200 calls/day at 1-minute polling (CTA default cap: 100,000/day).
# MAGIC
# MAGIC Bronze keeps CTA's own field names and string values, untouched. Renaming happens in silver.
# MAGIC
# MAGIC **First time:** store the Train Tracker key (`databricks secrets put-secret cta train_api_key`), then run this
# MAGIC notebook once by hand. It is a different key from the Bus Tracker one.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("polls_per_run", "1", "Polls per run")
dbutils.widgets.text("poll_interval_sec", "60", "Seconds between polls (when polls_per_run > 1)")
dbutils.widgets.dropdown("rediscover", "false", ["true", "false"], "Force stop discovery")

POLLS_PER_RUN = int(dbutils.widgets.get("polls_per_run"))
POLL_INTERVAL_SEC = int(dbutils.widgets.get("poll_interval_sec"))
REDISCOVER = dbutils.widgets.get("rediscover") == "true"

# COMMAND ----------

# DBTITLE 1,Setup (first run only)
def create_train_bronze_objects():
    """Train config, bronze, reference and ops tables. Idempotent, and safe to run before the bus notebooks."""
    for schema in [SCHEMA_CONFIG, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD, SCHEMA_OPS]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_BRONZE}.landing")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_OPS}.checkpoints")
    print("Schemas and volumes ready")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_TRAIN_DISCOVERY_RULES} (
      area          STRING  NOT NULL COMMENT 'Area name shown in the dashboard, e.g. Loop',
      min_latitude  DOUBLE  NOT NULL,
      max_latitude  DOUBLE  NOT NULL,
      min_longitude DOUBLE  NOT NULL,
      max_longitude DOUBLE  NOT NULL,
      is_active     BOOLEAN NOT NULL COMMENT 'Only active rules are used by stop discovery in the train bronze notebook',
      updated_at    TIMESTAMP
    )
    COMMENT 'Which bounding boxes define the L platforms we track. Every line serving a matching platform is included. Edit rows here to change coverage; no code changes needed.'
    """)

    # The Loop box is a touch taller than the bus one so Clark/Lake, State/Lake and LaSalle (Blue),
    # which sit right on the bus box edges, are safely inside.
    seed = [
        ("Loop", 41.8745, 41.8870, -87.6400, -87.6200, True),
        ("South Loop", 41.8500, 41.8745, -87.6350, -87.6150, False),
        ("West Loop", 41.8745, 41.8870, -87.6600, -87.6400, False),
    ]
    (spark.createDataFrame(seed, "area string, min_latitude double, max_latitude double, "
                                 "min_longitude double, max_longitude double, is_active boolean")
         .withColumn("updated_at", F.current_timestamp())
         .createOrReplaceTempView("seed_train_rules"))

    spark.sql(f"""
    MERGE INTO {TBL_TRAIN_DISCOVERY_RULES} AS t
    USING seed_train_rules AS s
    ON t.area = s.area
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Discovery rules ready")

    cta_columns = ",\n      ".join(f"{c} STRING" for c in TRAIN_PREDICTION_FIELDS)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_TRAIN_BRONZE_PREDICTIONS} (
      poll_id       STRING    COMMENT 'One API poll (all station batches requested at the same moment)',
      polled_at     TIMESTAMP COMMENT 'When we called the API',
      {cta_columns},
      _rescued_data STRING    COMMENT 'Fields CTA sent that are not in the schema (kept, never dropped)',
      _source_file  STRING    COMMENT 'Landing file this row came from',
      _ingested_at  TIMESTAMP COMMENT 'When Auto Loader wrote the row'
    )
    CLUSTER BY (polled_at)
    COMMENT 'Raw ttarrivals records. Column names and string values exactly as the CTA Train Tracker API returns them.'
    """)
    print(f"Ready: {TBL_TRAIN_BRONZE_PREDICTIONS}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_TRAIN_ROUTES} (
      route_id     STRING COMMENT 'Train Tracker route code, e.g. Brn',
      route_name   STRING COMMENT 'Line name, e.g. Brown Line',
      route_label  STRING COMMENT 'Display label, e.g. Brown Line',
      route_color  STRING COMMENT 'Hex color CTA uses for the line',
      refreshed_at TIMESTAMP
    )
    COMMENT 'The eight L lines (fixed list in _common), rewritten at each stop discovery'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_TRAIN_STOPS} (
      stop_id             STRING NOT NULL COMMENT 'CTA platform ID (stpId, 3xxxx): one direction at one station',
      station_id          STRING COMMENT 'CTA parent station ID (staId / mapid, 4xxxx); this is what we poll',
      stop_name           STRING COMMENT 'Station name, e.g. Clark/Lake',
      direction           STRING COMMENT 'Compass direction of travel at this platform, e.g. Northbound',
      stop_label          STRING COMMENT 'Display label with direction, e.g. Clark/Lake (Inner Loop)',
      area                STRING COMMENT 'Area from the discovery rule that found the platform',
      latitude            DOUBLE,
      longitude           DOUBLE,
      served_by_routes    ARRAY<STRING> COMMENT 'Lines that stop at this platform (route_id codes)',
      is_active           BOOLEAN COMMENT 'False once no active rule matches; ingestion stops polling its station',
      first_discovered_at TIMESTAMP,
      last_discovered_at  TIMESTAMP
    )
    COMMENT 'L platforms we track. Maintained by stop discovery in the train bronze notebook; rows are deactivated, never deleted, so history still joins.'
    """)
    print(f"Ready: {TBL_TRAIN_ROUTES}, {TBL_TRAIN_STOPS}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_TRAIN_INGESTION_RUNS} (
      run_id              STRING,
      started_at          TIMESTAMP,
      finished_at         TIMESTAMP,
      status              STRING  COMMENT 'success, partial (some API calls failed) or failed',
      stations_polled     INT,
      polls               INT,
      api_calls           INT,
      failed_api_calls    INT,
      predictions_landed  INT     COMMENT 'Prediction records written to the landing volume',
      bronze_rows_written BIGINT  COMMENT 'Rows Auto Loader appended to bronze this run',
      api_messages        STRING  COMMENT 'Distinct errors CTA returned (code: message)'
    )
    COMMENT 'One row per train bronze notebook run'
    """)
    print(f"Ready: {TBL_TRAIN_INGESTION_RUNS}")


# Only on the first run (or after a table was dropped), so the every-minute job stays fast.
_required = [TBL_TRAIN_DISCOVERY_RULES, TBL_TRAIN_BRONZE_PREDICTIONS, TBL_TRAIN_ROUTES, TBL_TRAIN_STOPS,
             TBL_TRAIN_INGESTION_RUNS]
if not all(spark.catalog.tableExists(t) for t in _required):
    create_train_bronze_objects()
else:
    print("Train bronze objects already exist")

# COMMAND ----------

# DBTITLE 1,Stop discovery (at most daily)
def _is_true(value):
    """Line flags in the stops dataset arrive as booleans or as "true"/"false" strings."""
    return value is True or str(value).strip().lower() in ("true", "1", "yes")


def _lat_lon(row):
    """Coordinates from the dataset's location field (a lat/lon object, or a GeoJSON point)."""
    loc = row.get("location") or {}
    if "latitude" in loc and "longitude" in loc:
        return float(loc["latitude"]), float(loc["longitude"])
    if loc.get("coordinates"):
        lon, lat = loc["coordinates"][:2]
        return float(lat), float(lon)
    return None


def match_train_stops(rows, rules):
    """Dataset rows + active rules -> {stop_id: platform attributes} for platforms inside a rule's box."""
    found = {}
    for row in rows:
        stop_id = str(row.get("stop_id") or "").strip()
        station_id = str(row.get("map_id") or row.get("station_id") or "").strip()
        coords = _lat_lon(row)
        # 11/12 add in if statement to filter for excluded stations
        if not stop_id or not station_id or coords is None or station_id in TRAIN_EXCLUDED_STATIONS:
            continue
        lat, lon = coords
        for rule in rules:
            inside = (rule.min_latitude <= lat <= rule.max_latitude
                      and rule.min_longitude <= lon <= rule.max_longitude)
            if not inside:
                continue
            direction_id = str(row.get("direction_id") or "").strip().upper()
            found.setdefault(stop_id, {
                "stop_id": stop_id,
                "station_id": station_id,
                "stop_name": row.get("station_name") or row.get("stop_name"),
                "direction": TRAIN_DIRECTION_NAMES.get(direction_id, direction_id or None),
                "stop_label": row.get("stop_name") or row.get("station_name"),
                "area": rule.area,
                "latitude": lat,
                "longitude": lon,
                "routes": sorted(route_id for route_id, _, _, flags in TRAIN_LINES
                                 if any(_is_true(row.get(flag)) for flag in flags)),
            })
    return found


def discover_train_stops():
    """Rewrite silver.train_routes and turn the discovery rules into silver.train_stops (1 Data Portal call)."""
    import requests

    (spark.createDataFrame([(r, name, name, color) for r, name, color, _ in TRAIN_LINES],
                           "route_id string, route_name string, route_label string, route_color string")
         .withColumn("refreshed_at", F.current_timestamp())
         .write.mode("overwrite").saveAsTable(TBL_TRAIN_ROUTES))

    rules = spark.table(TBL_TRAIN_DISCOVERY_RULES).filter("is_active").collect()
    if not rules:
        raise RuntimeError(f"No active rules in {TBL_TRAIN_DISCOVERY_RULES}")

    resp = requests.get(TRAIN_STOPS_URL, params={"$limit": 5000}, timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    print(f"Stops dataset returned {len(rows)} platforms system-wide")

    found = match_train_stops(rows, rules)
    if not found:
        # Never let an empty result deactivate every platform.
        raise RuntimeError("Discovery found no platforms; check the rules and the dataset before re-running.")
    print(f"{len(found)} platforms at {len({s['station_id'] for s in found.values()})} stations discovered")

    discovered = spark.createDataFrame(
        [(s["stop_id"], s["station_id"], s["stop_name"], s["direction"], s["stop_label"], s["area"],
          s["latitude"], s["longitude"], s["routes"]) for s in found.values()],
        "stop_id string, station_id string, stop_name string, direction string, stop_label string, area string, "
        "latitude double, longitude double, served_by_routes array<string>",
    ).withColumn("discovered_at", F.current_timestamp())
    discovered.createOrReplaceTempView("discovered_train_stops")

    spark.sql(f"""
    MERGE INTO {TBL_TRAIN_STOPS} AS t
    USING discovered_train_stops AS s
    ON t.stop_id = s.stop_id
    WHEN MATCHED THEN UPDATE SET
      station_id = s.station_id, stop_name = s.stop_name, direction = s.direction, stop_label = s.stop_label,
      area = s.area, latitude = s.latitude, longitude = s.longitude, served_by_routes = s.served_by_routes,
      is_active = true, last_discovered_at = s.discovered_at
    WHEN NOT MATCHED THEN INSERT
      (stop_id, station_id, stop_name, direction, stop_label, area, latitude, longitude, served_by_routes,
       is_active, first_discovered_at, last_discovered_at)
      VALUES (s.stop_id, s.station_id, s.stop_name, s.direction, s.stop_label, s.area, s.latitude, s.longitude,
              s.served_by_routes, true, s.discovered_at, s.discovered_at)
    WHEN NOT MATCHED BY SOURCE AND t.is_active THEN UPDATE SET is_active = false
    """)

    print(f"Active platforms: {spark.table(TBL_TRAIN_STOPS).filter('is_active').count()}")


# Runs at most once a day: when forced, when there are no platforms yet, or when the last discovery is
# more than 24 hours old. Stations rarely change, so a failed refresh is not worth stopping the poll for.
have_stops = spark.table(TBL_TRAIN_STOPS).filter("is_active").limit(1).count() > 0
stale = spark.sql(f"""
  SELECT coalesce(max(last_discovered_at) < current_timestamp() - INTERVAL 24 HOURS, true) AS stale
  FROM {TBL_TRAIN_STOPS} WHERE is_active
""").first().stale
if REDISCOVER or stale:
    try:
        discover_train_stops()
    except Exception as e:
        if REDISCOVER or not have_stops:
            raise
        print(f"Stop discovery failed ({type(e).__name__}: {e}); polling the platforms already on file")
else:
    print("Platforms discovered within the last 24 hours; skipping discovery")

# COMMAND ----------

# DBTITLE 1,Load active stations
active_stations = sorted(
    r.station_id for r in spark.table(TBL_TRAIN_STOPS).filter("is_active").select("station_id").distinct().collect()
)
if not active_stations:
    raise RuntimeError(f"No active platforms in {TBL_TRAIN_STOPS}. Re-run with rediscover = true.")
print(f"Polling {len(active_stations)} stations in {len(chunked(active_stations, MAX_STATIONS_PER_CALL))} API calls per poll")

# COMMAND ----------

# DBTITLE 1,Poll the API and land raw files
import json
import time
import uuid
from datetime import datetime, timezone

# Errors that will not fix themselves on the next call: stop the run instead of retrying into them.
FATAL_ERROR_CODES = {"101": "invalid API key", "102": "daily API limit reached"}


def request_arrivals(station_ids):
    """ttarrivals for up to MAX_STATIONS_PER_CALL stations. Returns (predictions, api_calls, failed_calls, messages).

    If CTA rejects a multi-station request, each station is retried on its own, so one bad
    station ID cannot blank out the others.
    """
    predictions, calls, failed, messages = [], 0, 0, set()
    pending = [list(station_ids)]
    while pending:
        group = pending.pop(0)
        calls += 1
        try:
            body = call_cta_train("ttarrivals.aspx", mapid=group)
        except RuntimeError as e:
            failed += 1
            messages.add(str(e))
            continue

        code = str(body.get("errCd") or "0")
        if code == "0":
            eta = body.get("eta") or []  # no trains predicted = no eta key at all
            predictions.extend([eta] if isinstance(eta, dict) else eta)
            continue

        message = f"{code}: {body.get('errNm')}"
        if code in FATAL_ERROR_CODES:
            raise RuntimeError(f"CTA says {message}. Stopping so the key is not hammered further.")
        failed += 1
        messages.add(message)
        if len(group) > 1:
            pending.extend([s] for s in group)
    return predictions, calls, failed, messages


def poll_once(station_ids):
    """One poll across all station batches. Returns (records, api_calls, failed_calls, messages)."""
    poll_id = uuid.uuid4().hex
    polled_at = datetime.now(timezone.utc)
    polled_at_ms = int(polled_at.timestamp() * 1000)
    records, api_calls, failed, messages = [], 0, 0, set()

    for batch in chunked(station_ids, MAX_STATIONS_PER_CALL):
        predictions, calls, batch_failed, batch_messages = request_arrivals(batch)
        api_calls += calls
        failed += batch_failed
        messages |= batch_messages
        for eta in predictions:
            # Values are stringified so bronze stays all-strings even if CTA starts sending numbers.
            record = {k: (v if v is None or isinstance(v, str) else json.dumps(v)) for k, v in eta.items()}
            records.append({**record, "_poll_id": poll_id, "_polled_at_ms": polled_at_ms})

    if records:
        day_dir = f"{TRAIN_LANDING_PATH}/{polled_at:%Y/%m/%d}"
        os.makedirs(day_dir, exist_ok=True)
        tmp_path = f"{day_dir}/.poll_{polled_at:%H%M%S}_{poll_id[:8]}.tmp"
        final_path = f"{day_dir}/poll_{polled_at:%H%M%S}_{poll_id[:8]}.json"
        with open(tmp_path, "w") as f:
            f.write("\n".join(json.dumps(r) for r in records) + "\n")
        os.rename(tmp_path, final_path)  # Auto Loader never sees a half-written file

    return records, api_calls, failed, messages


run_id = uuid.uuid4().hex
started_at = datetime.now(timezone.utc)
landed, api_calls, failed_calls, api_messages = 0, 0, 0, set()

for i in range(POLLS_PER_RUN):
    if i > 0:
        time.sleep(POLL_INTERVAL_SEC)
    records, calls, failed, messages = poll_once(active_stations)
    landed += len(records)
    api_calls += calls
    failed_calls += failed
    api_messages |= messages
    print(f"Poll {i + 1}/{POLLS_PER_RUN}: {len(records)} predictions, {failed}/{calls} calls failed")

# COMMAND ----------

# DBTITLE 1,Auto Loader: landing files -> bronze table
os.makedirs(TRAIN_LANDING_PATH, exist_ok=True)  # so the very first run works even if nothing was landed
rows_before = spark.table(TBL_TRAIN_BRONZE_PREDICTIONS).count()
query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "*.json")
    .schema(TRAIN_LANDING_SCHEMA)
    .load(TRAIN_LANDING_PATH)
    .select(
        F.col("_poll_id").alias("poll_id"),
        F.expr("timestamp_millis(_polled_at_ms)").alias("polled_at"),
        *TRAIN_PREDICTION_FIELDS,
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .writeStream
    .option("checkpointLocation", CHECKPOINT_TRAIN_BRONZE)
    .trigger(availableNow=True)
    .toTable(TBL_TRAIN_BRONZE_PREDICTIONS)
)
query.awaitTermination()
bronze_rows = spark.table(TBL_TRAIN_BRONZE_PREDICTIONS).count() - rows_before
print(f"Auto Loader appended {bronze_rows} rows to {TBL_TRAIN_BRONZE_PREDICTIONS}")

# COMMAND ----------

# DBTITLE 1,Log the run
status = "failed" if api_calls and failed_calls >= api_calls else ("partial" if failed_calls else "success")
run_row = [(
    run_id, started_at, datetime.now(timezone.utc), status, len(active_stations), POLLS_PER_RUN,
    api_calls, failed_calls, landed, bronze_rows, " | ".join(sorted(api_messages))[:2000] or None,
)]
(spark.createDataFrame(
    run_row,
    "run_id string, started_at timestamp, finished_at timestamp, status string, stations_polled int, polls int, "
    "api_calls int, failed_api_calls int, predictions_landed int, bronze_rows_written bigint, api_messages string",
 ).write.mode("append").saveAsTable(TBL_TRAIN_INGESTION_RUNS))

print(f"Run {run_id[:8]}: {status}")
if status == "failed":
    raise RuntimeError("Every CTA train API call failed this run; see ops.train_ingestion_runs.api_messages")