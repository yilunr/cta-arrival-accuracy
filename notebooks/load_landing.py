# Databricks notebook source
# MAGIC %md
# MAGIC # load_landing — hourly loader (first task of the transform job)
# MAGIC
# MAGIC One task, both modes, no child notebook runs. Does the heavy half of what `bronze` / `train_bronze`
# MAGIC used to do on every poll, once an hour:
# MAGIC
# MAGIC 1. Auto Loader: landed poll files -> `bronze.bus_predictions_raw` / `bronze.train_predictions_raw` (exactly once).
# MAGIC 2. Auto Loader: the poller's run-log files -> `ops.ingestion_runs` / `ops.train_ingestion_runs`.
# MAGIC 3. Rewrites the stop / station cache files that `poll_cta` reads.
# MAGIC 4. Once a day (when the stop list is more than 24 h old) runs `bronze` / `train_bronze` with
# MAGIC    `polls_per_run = 0` so stop discovery refreshes. That is the only time a child notebook runs.
# MAGIC
# MAGIC Widget `mode`: `both` in the single hourly job; `bus` or `train` only if you ever split the jobs again.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.dropdown("mode", "both", ["both", "train", "bus"], "Which landing to load")
MODES = ["bus", "train"] if dbutils.widgets.get("mode") == "both" else [dbutils.widgets.get("mode")]

import json

LANDING_ROOT = f"/Volumes/{CATALOG}/bronze/landing"
RUN_LOG_ROOT = f"{LANDING_ROOT}/run_logs"
STOP_CACHE = {"bus": f"{LANDING_ROOT}/active_bus_stops.json", "train": f"{LANDING_ROOT}/active_train_stations.json"}

RUN_LOG_SCHEMA = ("run_id string, started_at_ms long, finished_at_ms long, status string, polled int, polls int, "
                  "api_calls int, failed_api_calls int, predictions_landed int, api_messages string")

CFG = {
    "bus":   dict(landing=LANDING_PATH, schema=LANDING_SCHEMA, fields=CTA_PREDICTION_FIELDS,
                  checkpoint=CHECKPOINT_BRONZE, table=TBL_BRONZE_PREDICTIONS,
                  runs_table=TBL_INGESTION_RUNS, polled_col="stops_polled", stops_table=TBL_STOPS, notebook="./bronze"),
    "train": dict(landing=TRAIN_LANDING_PATH, schema=TRAIN_LANDING_SCHEMA, fields=TRAIN_PREDICTION_FIELDS,
                  checkpoint=CHECKPOINT_TRAIN_BRONZE, table=TBL_TRAIN_BRONZE_PREDICTIONS,
                  runs_table=TBL_TRAIN_INGESTION_RUNS, polled_col="stations_polled", stops_table=TBL_TRAIN_STOPS,
                  notebook="./train_bronze"),
}


def load_stream(src, schema, select_cols, checkpoint, table, glob="*.json"):
    """Auto Loader with availableNow: load every new file under src into table, exactly once."""
    os.makedirs(src, exist_ok=True)
    q = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("rescuedDataColumn", "_rescued_data")
         .option("pathGlobFilter", glob)
         .schema(schema)
         .load(src)
         .select(*select_cols)
         .writeStream
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .toTable(table))
    q.awaitTermination()


def write_json_atomic(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{os.path.dirname(path)}/.{os.path.basename(path)}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.rename(tmp, path)

# COMMAND ----------

# DBTITLE 1,1. Poll files -> bronze tables
for mode in MODES:
    c = CFG[mode]
    load_stream(
        c["landing"], c["schema"],
        [F.col("_poll_id").alias("poll_id"),
         F.expr("timestamp_millis(_polled_at_ms)").alias("polled_at"),
         *c["fields"],
         "_rescued_data",
         F.col("_metadata.file_path").alias("_source_file"),
         F.current_timestamp().alias("_ingested_at")],
        c["checkpoint"], c["table"],
    )
    print(f"[{mode}] poll files loaded into {c['table']}")

# COMMAND ----------

# DBTITLE 1,2. Run-log files -> ops tables
for mode in MODES:
    c = CFG[mode]
    load_stream(
        f"{RUN_LOG_ROOT}/{mode}", RUN_LOG_SCHEMA,
        ["run_id",
         F.expr("timestamp_millis(started_at_ms)").alias("started_at"),
         F.expr("timestamp_millis(finished_at_ms)").alias("finished_at"),
         "status",
         F.col("polled").alias(c["polled_col"]),
         "polls", "api_calls", "failed_api_calls", "predictions_landed",
         F.lit(None).cast("bigint").alias("bronze_rows_written"),
         "api_messages"],
        f"{CHECKPOINT_ROOT}/ingestion_runs_{mode}", c["runs_table"],
    )
    print(f"[{mode}] run logs loaded into {c['runs_table']}")

# COMMAND ----------

# DBTITLE 1,3. Stop / station cache for the poller, and 4. daily discovery refresh
for mode in MODES:
    c = CFG[mode]
    stops = spark.table(c["stops_table"]).filter("is_active")
    id_col = "stop_id" if mode == "bus" else "station_id"
    ids = sorted({r[id_col] for r in stops.select(id_col).collect()})
    if ids:
        write_json_atomic(STOP_CACHE[mode], ids)
        print(f"[{mode}] cache: {len(ids)} {'stops' if mode == 'bus' else 'stations'}")
    else:
        print(f"[{mode}] WARNING: no active stops; cache left unchanged")

    # Discovery costs ~25 API calls and a child notebook run, so only when the stop list is more than a day old.
    stale = spark.sql(f"""
      SELECT coalesce(max(last_discovered_at) < current_timestamp() - INTERVAL 24 HOURS, true) AS stale
      FROM {c['stops_table']} WHERE is_active
    """).first().stale
    if stale:
        print(f"[{mode}] stop list is more than 24 h old; running {c['notebook']} for discovery ...")
        print(f"[{mode}] {c['notebook']} -> {dbutils.notebook.run(c['notebook'], 1800, {'polls_per_run': '0'})}")
