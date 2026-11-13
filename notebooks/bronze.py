# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · Ingest CTA bus predictions (job: every 1-2 minutes)
# MAGIC
# MAGIC One notebook for everything that touches the CTA API:
# MAGIC
# MAGIC 1. **Setup**: creates schemas, volumes and the bronze, config, reference and ops tables on the first run.
# MAGIC 2. **Stop discovery** (at most daily): turns `config.stop_discovery_rules` (route + bounding box) into `silver.stops`.
# MAGIC    To change coverage, edit the rules table and set `rediscover = true` once.
# MAGIC 3. **Poll**: calls `getpredictions` for every active stop, 10 stops per call, and lands each poll as a JSON-lines file in the `bronze.landing` volume.
# MAGIC 4. **Auto Loader** (`availableNow`): loads new files into `bronze.bus_predictions_raw` exactly once.
# MAGIC 5. **Run log**: one row per run in `ops.ingestion_runs`.
# MAGIC
# MAGIC Micro-batch streaming: the Job schedule sets the cadence; each run processes what's new and shuts down, so no compute sits idle.
# MAGIC **Job settings:** every 1-2 min, max concurrent runs = 1, 1 retry. 32 stops = 4 calls per poll, about 2,900 calls/day at 2-minute polling (CTA default cap: 10,000/day).
# MAGIC
# MAGIC Bronze keeps CTA's own field names and string values, untouched. Renaming happens in silver.
# MAGIC
# MAGIC **First time:** store the key (`databricks secrets put-secret cta api_key`), run this notebook once by hand, then optionally once with `backfill_legacy = true`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("polls_per_run", "1", "Polls per run")
dbutils.widgets.text("poll_interval_sec", "60", "Seconds between polls (when polls_per_run > 1)")
dbutils.widgets.dropdown("rediscover", "false", ["true", "false"], "Force stop discovery")
dbutils.widgets.dropdown("backfill_legacy", "false", ["true", "false"], "Copy pre-refactor bronze rows (one time)")

POLLS_PER_RUN = int(dbutils.widgets.get("polls_per_run"))
POLL_INTERVAL_SEC = int(dbutils.widgets.get("poll_interval_sec"))
REDISCOVER = dbutils.widgets.get("rediscover") == "true"
BACKFILL_LEGACY = dbutils.widgets.get("backfill_legacy") == "true"

# COMMAND ----------

# DBTITLE 1,Setup (first run only)
def create_bronze_objects():
    """Schemas, volumes, config, bronze, reference and ops tables. Idempotent."""
    for schema, comment in [
        (SCHEMA_CONFIG, "Pipeline configuration you edit by hand"),
        (SCHEMA_BRONZE, "Raw CTA Bus Tracker data exactly as the API returned it"),
        (SCHEMA_SILVER, "Cleaned, renamed and modeled bus data"),
        (SCHEMA_GOLD, "Dashboard-ready aggregates"),
        (SCHEMA_OPS, "Pipeline run logs and streaming checkpoints"),
    ]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comment}'")

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_BRONZE}.landing COMMENT 'Raw getpredictions responses, one JSON-lines file per poll'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA_OPS}.checkpoints COMMENT 'Structured Streaming checkpoints'")
    print("Schemas and volumes ready")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_DISCOVERY_RULES} (
      route_id      STRING  NOT NULL COMMENT 'CTA route used to find stops, e.g. 146',
      area          STRING  NOT NULL COMMENT 'Area name shown in the dashboard, e.g. Loop',
      min_latitude  DOUBLE  NOT NULL,
      max_latitude  DOUBLE  NOT NULL,
      min_longitude DOUBLE  NOT NULL,
      max_longitude DOUBLE  NOT NULL,
      is_active     BOOLEAN NOT NULL COMMENT 'Only active rules are used by stop discovery in the bronze notebook',
      updated_at    TIMESTAMP
    )
    COMMENT 'Which routes and bounding boxes define the stops we track. Edit rows here to change coverage; no code changes needed.'
    """)

    LOOP = (41.8750, 41.8860, -87.6400, -87.6200)
    SOUTH_LOOP = (41.8500, 41.8750, -87.6350, -87.6150)
    seed = (
        [(rt, "Loop", *LOOP, True) for rt in ["2", "6", "10", "29", "62", "146", "147", "148"]]
        + [(rt, "South Loop", *SOUTH_LOOP, False) for rt in ["2", "6"]]
    )
    (spark.createDataFrame(seed, "route_id string, area string, min_latitude double, max_latitude double, "
                                 "min_longitude double, max_longitude double, is_active boolean")
         .withColumn("updated_at", F.current_timestamp())
         .createOrReplaceTempView("seed_rules"))

    spark.sql(f"""
    MERGE INTO {TBL_DISCOVERY_RULES} AS t
    USING seed_rules AS s
    ON t.route_id = s.route_id AND t.area = s.area
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("Discovery rules ready")

    cta_columns = ",\n  ".join(f"{c} STRING" for c in CTA_PREDICTION_FIELDS)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_BRONZE_PREDICTIONS} (
      poll_id       STRING    COMMENT 'One API poll (all stop batches requested at the same moment)',
      polled_at     TIMESTAMP COMMENT 'When we called the API',
      {cta_columns},
      _rescued_data STRING    COMMENT 'Fields CTA sent that are not in the schema (kept, never dropped)',
      _source_file  STRING    COMMENT 'Landing file this row came from',
      _ingested_at  TIMESTAMP COMMENT 'When Auto Loader wrote the row'
    )
    CLUSTER BY (polled_at)
    COMMENT 'Raw getpredictions records. Column names and string values exactly as the CTA API returns them.'
    """)
    print(f"Ready: {TBL_BRONZE_PREDICTIONS}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_ROUTES} (
      route_id     STRING COMMENT 'CTA route number, e.g. 146',
      route_name   STRING COMMENT 'CTA route name, e.g. Inner Drive/Michigan Express',
      route_label  STRING COMMENT 'Display label, e.g. 146 · Inner Drive/Michigan Express',
      route_color  STRING COMMENT 'Hex color CTA uses for the route',
      refreshed_at TIMESTAMP
    )
    COMMENT 'All CTA bus routes (from getroutes), refreshed daily'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_STOPS} (
      stop_id               STRING NOT NULL COMMENT 'CTA stop ID',
      stop_name             STRING COMMENT 'CTA stop name, e.g. State & Madison',
      direction             STRING COMMENT 'Direction of travel served at this pole, e.g. Northbound',
      stop_label            STRING COMMENT 'Display label with direction, e.g. State & Madison (NB)',
      area                  STRING COMMENT 'Area from the discovery rule that found the stop',
      latitude              DOUBLE,
      longitude             DOUBLE,
      discovered_via_routes ARRAY<STRING> COMMENT 'Routes whose discovery rules matched this stop',
      is_active             BOOLEAN COMMENT 'False once no active rule matches; ingestion stops polling it',
      first_discovered_at   TIMESTAMP,
      last_discovered_at    TIMESTAMP
    )
    COMMENT 'Stops we poll. Maintained by stop discovery in the bronze notebook; rows are deactivated, never deleted, so history still joins.'
    """)
    print(f"Ready: {TBL_ROUTES}, {TBL_STOPS}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_INGESTION_RUNS} (
      run_id              STRING,
      started_at          TIMESTAMP,
      finished_at         TIMESTAMP,
      status              STRING  COMMENT 'success, partial (some API calls failed) or failed',
      stops_polled        INT,
      polls               INT,
      api_calls           INT,
      failed_api_calls    INT,
      predictions_landed  INT     COMMENT 'Prediction records written to the landing volume',
      bronze_rows_written BIGINT  COMMENT 'Rows Auto Loader appended to bronze this run',
      api_messages        STRING  COMMENT 'Distinct messages CTA returned (e.g. No service scheduled)'
    )
    COMMENT 'One row per bronze notebook run'
    """)
    print(f"Ready: {TBL_INGESTION_RUNS}")


# Only on the first run (or after a table was dropped), so the every-minute job stays fast.
_required = [TBL_DISCOVERY_RULES, TBL_BRONZE_PREDICTIONS, TBL_ROUTES, TBL_STOPS, TBL_INGESTION_RUNS]
if not all(spark.catalog.tableExists(t) for t in _required):
    create_bronze_objects()
else:
    print("Bronze objects already exist")

# COMMAND ----------

# DBTITLE 1,Stop discovery (at most daily)
def discover_stops():
    """Refresh silver.routes and turn the discovery rules into silver.stops (~25 API calls)."""
    routes_body = call_cta("getroutes")
    routes = [
        (r["rt"], r.get("rtnm"), f"{r['rt']} · {r.get('rtnm', '')}".strip(" ·"), r.get("rtclr"))
        for r in routes_body.get("routes", [])
    ]
    if not routes:
        raise RuntimeError(f"getroutes returned no routes: {routes_body.get('error')}")

    (spark.createDataFrame(routes, "route_id string, route_name string, route_label string, route_color string")
         .withColumn("refreshed_at", F.current_timestamp())
         .write.mode("overwrite").saveAsTable(TBL_ROUTES))
    print(f"Refreshed {len(routes)} routes")

    rules = spark.table(TBL_DISCOVERY_RULES).filter("is_active").collect()
    if not rules:
        raise RuntimeError(f"No active rules in {TBL_DISCOVERY_RULES}")

    found = {}  # stop_id -> stop attributes
    for route_id in sorted({r.route_id for r in rules}):
        route_rules = [r for r in rules if r.route_id == route_id]
        directions = [d["dir"] for d in call_cta("getdirections", rt=route_id).get("directions", [])]
        matched = 0
        for direction in directions:
            for stop in call_cta("getstops", rt=route_id, dir=direction).get("stops", []):
                lat, lon = float(stop["lat"]), float(stop["lon"])
                for rule in route_rules:
                    inside = (rule.min_latitude <= lat <= rule.max_latitude
                              and rule.min_longitude <= lon <= rule.max_longitude)
                    if not inside:
                        continue
                    matched += 1
                    entry = found.setdefault(stop["stpid"], {
                        "stop_id": stop["stpid"],
                        "stop_name": stop["stpnm"],
                        "direction": direction,
                        "area": rule.area,
                        "latitude": lat,
                        "longitude": lon,
                        "routes": set(),
                    })
                    entry["routes"].add(route_id)
        print(f"Route {route_id}: {matched} stop matches across {len(directions)} directions")

    if not found:
        # Never let an empty result deactivate every stop.
        raise RuntimeError("Discovery found no stops; check the rules and the API before re-running.")
    print(f"\n{len(found)} unique stops discovered")

    discovered = (
        spark.createDataFrame(
            [(s["stop_id"], s["stop_name"], s["direction"], s["area"], s["latitude"], s["longitude"], sorted(s["routes"]))
             for s in found.values()],
            "stop_id string, stop_name string, direction string, area string, latitude double, longitude double, "
            "discovered_via_routes array<string>",
        )
        .withColumn("stop_label", F.concat(F.col("stop_name"), F.lit(" (#"), F.col("stop_id"), F.lit(")")))
        .withColumn("discovered_at", F.current_timestamp())
    )
    discovered.createOrReplaceTempView("discovered_stops")

    spark.sql(f"""
    MERGE INTO {TBL_STOPS} AS t
    USING discovered_stops AS s
    ON t.stop_id = s.stop_id
    WHEN MATCHED THEN UPDATE SET
      stop_name = s.stop_name, direction = s.direction, stop_label = s.stop_label, area = s.area,
      latitude = s.latitude, longitude = s.longitude, discovered_via_routes = s.discovered_via_routes,
      is_active = true, last_discovered_at = s.discovered_at
    WHEN NOT MATCHED THEN INSERT
      (stop_id, stop_name, direction, stop_label, area, latitude, longitude, discovered_via_routes,
       is_active, first_discovered_at, last_discovered_at)
      VALUES (s.stop_id, s.stop_name, s.direction, s.stop_label, s.area, s.latitude, s.longitude,
              s.discovered_via_routes, true, s.discovered_at, s.discovered_at)
    WHEN NOT MATCHED BY SOURCE AND t.is_active THEN UPDATE SET is_active = false
    """)

    print(f"Active stops: {spark.table(TBL_STOPS).filter('is_active').count()}")


# Discovery costs ~25 API calls, so it runs at most once a day: when forced, when there are no stops yet,
# or when the last discovery is more than 24 hours old. CTA asks developers to refresh stop lists about daily.
stale = spark.sql(f"""
  SELECT coalesce(max(last_discovered_at) < current_timestamp() - INTERVAL 24 HOURS, true) AS stale
  FROM {TBL_STOPS} WHERE is_active
""").first().stale
if REDISCOVER or stale:
    discover_stops()
else:
    print("Stops discovered within the last 24 hours; skipping discovery")

# COMMAND ----------

# DBTITLE 1,Load active stops
active_stops = [r.stop_id for r in spark.table(TBL_STOPS).filter("is_active").select("stop_id").collect()]
if not active_stops:
    raise RuntimeError(f"No active stops in {TBL_STOPS}. Re-run with rediscover = true.")
print(f"Polling {len(active_stops)} stops in {len(chunked(active_stops, MAX_STOPS_PER_CALL))} API calls per poll")

# COMMAND ----------

# DBTITLE 1,Poll the API and land raw files
import json
import time
import uuid
from datetime import datetime, timezone


def poll_once(stop_ids):
    """One poll across all stop batches. Returns (records, api_calls, failed_calls, messages)."""
    poll_id = uuid.uuid4().hex
    polled_at = datetime.now(timezone.utc)
    polled_at_ms = int(polled_at.timestamp() * 1000)
    records, failed, messages = [], 0, set()

    batches = chunked(stop_ids, MAX_STOPS_PER_CALL)
    for batch in batches:
        try:
            body = call_cta("getpredictions", stpid=",".join(batch))
        except RuntimeError as e:
            failed += 1
            messages.add(str(e))
            continue
        # A batch can return predictions AND per-stop errors (e.g. one stop has no service right now).
        # Keep the predictions; record the messages.
        for err in body.get("error", []):
            msg = err.get("msg", "unknown error")
            messages.add(msg)
            if "limit" in msg.lower():
                raise RuntimeError(f"CTA says: {msg}. Stopping so the daily key quota is not burned further.")
        for prd in body.get("prd", []):
            records.append({**prd, "_poll_id": poll_id, "_polled_at_ms": polled_at_ms})

    if records:
        day_dir = f"{LANDING_PATH}/{polled_at:%Y/%m/%d}"
        os.makedirs(day_dir, exist_ok=True)
        tmp_path = f"{day_dir}/.poll_{polled_at:%H%M%S}_{poll_id[:8]}.tmp"
        final_path = f"{day_dir}/poll_{polled_at:%H%M%S}_{poll_id[:8]}.json"
        with open(tmp_path, "w") as f:
            f.write("\n".join(json.dumps(r) for r in records) + "\n")
        os.rename(tmp_path, final_path)  # Auto Loader never sees a half-written file

    return records, len(batches), failed, messages


run_id = uuid.uuid4().hex
started_at = datetime.now(timezone.utc)
landed, api_calls, failed_calls, api_messages = 0, 0, 0, set()

for i in range(POLLS_PER_RUN):
    if i > 0:
        time.sleep(POLL_INTERVAL_SEC)
    records, calls, failed, messages = poll_once(active_stops)
    landed += len(records)
    api_calls += calls
    failed_calls += failed
    api_messages |= messages
    print(f"Poll {i + 1}/{POLLS_PER_RUN}: {len(records)} predictions, {failed}/{calls} calls failed")

# COMMAND ----------

# DBTITLE 1,Auto Loader: landing files -> bronze table
rows_before = spark.table(TBL_BRONZE_PREDICTIONS).count()
query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "*.json")
    .schema(LANDING_SCHEMA)
    .load(LANDING_PATH)
    .select(
        F.col("_poll_id").alias("poll_id"),
        F.expr("timestamp_millis(_polled_at_ms)").alias("polled_at"),
        *CTA_PREDICTION_FIELDS,
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    .writeStream
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .toTable(TBL_BRONZE_PREDICTIONS)
)
query.awaitTermination()
bronze_rows = spark.table(TBL_BRONZE_PREDICTIONS).count() - rows_before
print(f"Auto Loader appended {bronze_rows} rows to {TBL_BRONZE_PREDICTIONS}")

# COMMAND ----------

# DBTITLE 1,Log the run
status = "failed" if api_calls and failed_calls == api_calls else ("partial" if failed_calls else "success")
run_row = [(
    run_id, started_at, datetime.now(timezone.utc), status, len(active_stops), POLLS_PER_RUN,
    api_calls, failed_calls, landed, bronze_rows, " | ".join(sorted(api_messages))[:2000] or None,
)]
(spark.createDataFrame(
    run_row,
    "run_id string, started_at timestamp, finished_at timestamp, status string, stops_polled int, polls int, "
    "api_calls int, failed_api_calls int, predictions_landed int, bronze_rows_written bigint, api_messages string",
 ).write.mode("append").saveAsTable(TBL_INGESTION_RUNS))

print(f"Run {run_id[:8]}: {status}")
if status == "failed":
    raise RuntimeError("Every CTA API call failed this run; see ops.ingestion_runs.api_messages")

# COMMAND ----------

# DBTITLE 1,One-time: backfill pre-refactor data
if BACKFILL_LEGACY and spark.catalog.tableExists(TBL_LEGACY_BRONZE):
    already_loaded = spark.table(TBL_BRONZE_PREDICTIONS).filter(F.col("poll_id").startswith("legacy-")).limit(1).count()

    if already_loaded:
        print("Legacy rows are already in the new bronze table; skipping. Delete those rows first to reload.")
    else:
        legacy = (
            spark.table(TBL_LEGACY_BRONZE)
            .select(
                F.concat(F.lit("legacy-"), F.expr("cast(unix_millis(ingest_ts) as string)")).alias("poll_id"),
                F.col("ingest_ts").alias("polled_at"),
                F.from_json("raw_json", CTA_PREDICTION_SCHEMA).alias("p"),
            )
            .select(
                "poll_id",
                "polled_at",
                *[F.col(f"p.{c}").alias(c) for c in CTA_PREDICTION_FIELDS],
                F.lit(None).cast("string").alias("_rescued_data"),
                F.lit(f"legacy:{TBL_LEGACY_BRONZE}").alias("_source_file"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )
        legacy.write.mode("append").saveAsTable(TBL_BRONZE_PREDICTIONS)
        print(f"Copied {spark.table(TBL_BRONZE_PREDICTIONS).filter(F.col('poll_id').startswith('legacy-')).count():,} legacy rows")
else:
    print("Legacy backfill not requested")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the old Genie-era tables
# MAGIC Uncomment once the new silver and gold tables look right.