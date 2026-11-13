# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Train · Silver · Clean predictions and score them against real arrivals (job: hourly)
# MAGIC
# MAGIC The train twin of `silver`. Same three tables, same scoring rules, same time fields.
# MAGIC
# MAGIC | Table | Grain | How it's built |
# MAGIC |---|---|---|
# MAGIC | `silver.train_predictions` | one prediction per poll | Structured Streaming from bronze (`availableNow`), typed and renamed |
# MAGIC | `silver.train_arrivals` | one train visit to a platform | rebuilt for the latest dates with `replaceWhere` |
# MAGIC | `silver.train_prediction_outcomes` | one prediction scored against the real arrival | same |
# MAGIC
# MAGIC ### How an arrival is detected
# MAGIC Like Bus Tracker, Train Tracker never reports actual arrivals, so we infer them the same way. A **visit** is one
# MAGIC run (train) on one line approaching one platform. When the run stops appearing in that platform's predictions,
# MAGIC it has served the platform. The visit gets one of three statuses:
# MAGIC
# MAGIC * **arrived**: the next poll of the platform came within `MAX_NEXT_POLL_GAP_SEC`, and on its last sighting the
# MAGIC   train was flagged Approaching/Due or predicted within `TRAIN_ARRIVAL_MAX_LAST_WAIT_MIN`.
# MAGIC   Arrival time = midpoint between the two polls.
# MAGIC * **vanished**: the run dropped out while still several minutes away (rerouted, run number changed, held). Not scored.
# MAGIC * **censored**: polling stopped or had a gap, so we never saw it arrive. Not scored.
# MAGIC
# MAGIC Visits still in progress at the latest poll are skipped and picked up next run.
# MAGIC Only `arrived` visits feed the accuracy numbers.
# MAGIC
# MAGIC ### What's different from buses
# MAGIC * **No trip ID.** A run number comes back to the same platform on its next round trip, so visits are split when
# MAGIC   a run has not been predicted at a platform for `VISIT_SPLIT_GAP_MIN`.
# MAGIC * **Scheduled predictions.** Rows with `is_scheduled` come from the timetable, not a live train (typically a
# MAGIC   train that has not left its terminal yet). They stay in `silver.train_predictions` but are left out of
# MAGIC   arrivals and scoring unless `TRAIN_SCORE_SCHEDULED` is turned on in `_common`.
# MAGIC * **`direction`** is CTA's platform description (e.g. "Service toward Loop"), since trains have no
# MAGIC   Northbound/Southbound field in the prediction itself.
# MAGIC * **A known lean.** A train can stay listed as Due while it sits at the platform, so the inferred arrival can
# MAGIC   land a few seconds after the doors opened. It is well inside the 3-minute on-time window.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Rebuild all history")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

# DBTITLE 1,Tables
time_columns = """
  local_date  DATE    COMMENT 'Chicago calendar date',
  hour_of_day INT     COMMENT 'Chicago hour, 0-23',
  hour_label  STRING  COMMENT 'Hour as text, e.g. 07:00',
  day_of_week STRING  COMMENT 'Sortable day label, e.g. 1 Mon',
  day_type    STRING  COMMENT 'Weekday or Weekend',
  time_period STRING  COMMENT 'Part of day, e.g. 07-10 AM rush'"""

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_PREDICTIONS} (
  poll_id                 STRING,
  polled_at               TIMESTAMP COMMENT 'When we called the API (the moment a rider would have looked)',
  prediction_generated_at TIMESTAMP COMMENT 'CTA prdt: when CTA produced the prediction',
  predicted_arrival_at    TIMESTAMP COMMENT 'CTA arrT: when CTA said the train would arrive',
  station_id              STRING    COMMENT 'CTA staId: parent station',
  stop_id                 STRING    COMMENT 'CTA stpId: the platform (one direction at one station)',
  stop_name               STRING    COMMENT 'CTA staNm: station name',
  run_number              STRING    COMMENT 'CTA rn: the train run; reused on later round trips',
  route_id                STRING    COMMENT 'CTA rt: line code, e.g. Brn',
  direction               STRING    COMMENT 'CTA stpDe: platform description, e.g. Service toward Loop',
  direction_code          STRING    COMMENT 'CTA trDr: operational direction code (1 or 5; meaning depends on the line)',
  destination             STRING    COMMENT 'CTA destNm: where the train is headed',
  destination_stop_id     STRING    COMMENT 'CTA destSt',
  is_approaching          BOOLEAN   COMMENT 'CTA isApp: Train Tracker was showing Approaching/Due',
  is_scheduled            BOOLEAN   COMMENT 'CTA isSch: prediction based on the timetable, not a live train',
  is_delayed              BOOLEAN   COMMENT 'CTA isDly: train has not moved for an abnormally long time',
  is_fault                BOOLEAN   COMMENT 'CTA isFlt: CTA flagged a potential fault with this prediction',
  latitude                DOUBLE    COMMENT 'Train position when the prediction was made',
  longitude               DOUBLE,
  heading_deg             INT       COMMENT 'Train heading, degrees clockwise from north',
  local_date              DATE      COMMENT 'Chicago date of polled_at',
  _ingested_at            TIMESTAMP
)
CLUSTER BY (local_date, stop_id)
COMMENT 'One row per train prediction per poll, typed and renamed from bronze'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_ARRIVALS} (
  arrival_id              STRING    COMMENT 'Hash of run, line, platform and first sighting',
  run_number              STRING,
  route_id                STRING,
  direction               STRING,
  destination             STRING,
  station_id              STRING,
  stop_id                 STRING,
  stop_name               STRING,
  first_seen_at           TIMESTAMP COMMENT 'First poll that predicted this train at this platform',
  last_seen_at            TIMESTAMP COMMENT 'Last poll that still predicted it',
  observation_count       INT       COMMENT 'Polls in which the train was predicted for this platform',
  last_predicted_wait_min DOUBLE    COMMENT 'Minutes to arrival the tracker showed on the last sighting',
  was_approaching         BOOLEAN   COMMENT 'Flagged Approaching/Due on the last sighting',
  next_polled_at          TIMESTAMP COMMENT 'Next poll of this platform after the last sighting',
  arrival_status          STRING    COMMENT 'arrived, vanished (dropped out while still minutes away) or censored (polling gap, arrival not observed)',
  actual_arrival_at       TIMESTAMP COMMENT 'Midpoint between last sighting and next poll; only when arrival_status = arrived',
  {time_columns}
)
CLUSTER BY (local_date)
COMMENT 'One row per train visit to a platform. Time fields use the arrival time (or last sighting when not arrived).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_OUTCOMES} (
  arrival_id            STRING,
  poll_id               STRING,
  polled_at             TIMESTAMP,
  route_id              STRING,
  direction             STRING,
  station_id            STRING,
  stop_id               STRING,
  stop_name             STRING,
  run_number            STRING,
  predicted_arrival_at  TIMESTAMP,
  actual_arrival_at     TIMESTAMP,
  predicted_wait_min    DOUBLE  COMMENT 'What the tracker said: minutes from polled_at to predicted arrival',
  actual_wait_min       DOUBLE  COMMENT 'Minutes from polled_at to the actual arrival',
  arrival_error_min     DOUBLE  COMMENT 'actual minus predicted; positive = train came later than promised',
  abs_error_min         DOUBLE,
  predicted_wait_bucket STRING  COMMENT 'e.g. 05-10 min',
  prediction_outcome    STRING  COMMENT 'early, on_time or late (tolerances in _common, same as bus)',
  is_delayed            BOOLEAN,
  is_scheduled          BOOLEAN COMMENT 'Always false unless TRAIN_SCORE_SCHEDULED is on',
  {time_columns}
)
CLUSTER BY (local_date)
COMMENT 'Every train prediction for a confirmed arrival, scored against what actually happened. Time fields use polled_at.'
""")
print(f"Ready: {TBL_TRAIN_PREDICTIONS}, {TBL_TRAIN_ARRIVALS}, {TBL_TRAIN_OUTCOMES}")

# COMMAND ----------

# DBTITLE 1,Transform functions
TIME_COLUMNS = ["local_date", "hour_of_day", "hour_label", "day_of_week", "day_type", "time_period"]

VISIT_COLUMNS = [
    "arrival_id", "run_number", "route_id", "direction", "destination", "station_id", "stop_id", "stop_name",
    "first_seen_at", "last_seen_at", "observation_count", "last_predicted_wait_min", "was_approaching",
    "next_polled_at", "arrival_status", "actual_arrival_at", *TIME_COLUMNS,
]

OUTCOME_COLUMNS = [
    "arrival_id", "poll_id", "polled_at", "route_id", "direction", "station_id", "stop_id", "stop_name",
    "run_number", "predicted_arrival_at", "actual_arrival_at", "predicted_wait_min", "actual_wait_min",
    "arrival_error_min", "abs_error_min", "predicted_wait_bucket", "prediction_outcome", "is_delayed",
    "is_scheduled", *TIME_COLUMNS,
]


def clean_train_predictions(bronze):
    """Bronze (CTA names, all strings) -> typed, renamed silver columns. Stateless, so it works on a stream."""
    return bronze.select(
        "poll_id",
        "polled_at",
        parse_train_time("prdt").alias("prediction_generated_at"),
        parse_train_time("arrT").alias("predicted_arrival_at"),
        clean_str("staId").alias("station_id"),
        clean_str("stpId").alias("stop_id"),
        clean_str("staNm").alias("stop_name"),
        clean_str("rn").alias("run_number"),
        clean_str("rt").alias("route_id"),
        clean_str("stpDe").alias("direction"),
        clean_str("trDr").alias("direction_code"),
        clean_str("destNm").alias("destination"),
        clean_str("destSt").alias("destination_stop_id"),
        train_flag("isApp").alias("is_approaching"),
        train_flag("isSch").alias("is_scheduled"),
        train_flag("isDly").alias("is_delayed"),
        train_flag("isFlt").alias("is_fault"),
        F.expr("try_cast(lat AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(lon AS DOUBLE)").alias("longitude"),
        F.expr("try_cast(heading AS INT)").alias("heading_deg"),
        F.to_date("polled_at").alias("local_date"),
        "_ingested_at",
    )


def build_train_visits(preds):
    """Group predictions into train visits and decide how each one ended.

    Returns (observations, visits):
      observations: each scoreable prediction tagged with its arrival_id
      visits:       one row per finished visit, with arrival_status and actual_arrival_at
    """
    # Every poll in which a platform returned any prediction (scheduled ones included):
    # tells us when we next looked at the platform.
    stop_polls = (
        preds.filter(F.col("stop_id").isNotNull())
        .select("stop_id", F.col("polled_at").alias("last_seen_at")).distinct()
        .withColumn("next_polled_at", F.lead("last_seen_at").over(Window.partitionBy("stop_id").orderBy("last_seen_at")))
    )
    data_end = preds.agg(F.max("polled_at").alias("data_end_at"))

    obs = preds.filter(
        F.col("run_number").isNotNull()
        & F.col("stop_id").isNotNull()
        & F.col("predicted_arrival_at").isNotNull()
    )
    if not TRAIN_SCORE_SCHEDULED:
        obs = obs.filter(~F.col("is_scheduled"))
    obs = (
        obs.withColumn("route_key", F.coalesce("route_id", F.lit("?")))
        .dropDuplicates(["poll_id", "run_number", "route_key", "stop_id"])
    )

    # One visit = run x line x platform. There is no trip ID, and a run number returns to the same
    # platform on its next round trip, so a long gap starts a new visit.
    visit_keys = ["run_number", "route_key", "stop_id"]
    w_visit = Window.partitionBy(*visit_keys).orderBy("polled_at")
    obs = (
        obs.withColumn("prev_polled_at", F.lag("polled_at").over(w_visit))
        .withColumn(
            "starts_visit",
            (F.col("prev_polled_at").isNull()
             | (minutes_between("polled_at", "prev_polled_at") > VISIT_SPLIT_GAP_MIN)).cast("int"),
        )
        .withColumn("visit_seq", F.sum("starts_visit").over(w_visit.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("visit_start_at", F.min("polled_at").over(Window.partitionBy(*visit_keys, "visit_seq")))
        .withColumn(
            "arrival_id",
            F.substring(F.sha2(F.concat_ws(
                "|", F.lit("train"), "run_number", "route_key", "stop_id",
                F.expr("cast(unix_micros(visit_start_at) as string)"),
            ), 256), 1, 20),
        )
    )

    last_sighting = (
        obs.withColumn("rn", F.row_number().over(Window.partitionBy("arrival_id").orderBy(F.desc("polled_at"))))
        .filter("rn = 1")
        .select(
            "arrival_id", "route_id", "direction", "destination", "station_id", "stop_name",
            F.greatest(F.lit(0.0), minutes_between("predicted_arrival_at", "polled_at")).alias("last_predicted_wait_min"),
            F.col("is_approaching").alias("was_approaching"),
        )
    )

    visits = (
        obs.groupBy("arrival_id", "run_number", "stop_id")
        .agg(
            F.min("polled_at").alias("first_seen_at"),
            F.max("polled_at").alias("last_seen_at"),
            F.count("*").cast("int").alias("observation_count"),
        )
        .join(last_sighting, "arrival_id")
        .join(stop_polls, ["stop_id", "last_seen_at"], "left")
        .crossJoin(F.broadcast(data_end))
    )

    gap_to_next_poll_sec = F.expr("(unix_micros(next_polled_at) - unix_micros(last_seen_at)) / 1.0E6")
    sec_before_data_end = F.expr("(unix_micros(data_end_at) - unix_micros(last_seen_at)) / 1.0E6")
    close_on_last_sighting = (
        F.col("was_approaching") | (F.col("last_predicted_wait_min") <= TRAIN_ARRIVAL_MAX_LAST_WAIT_MIN)
    )

    visits = visits.withColumn(
        "arrival_status",
        F.when(F.col("next_polled_at").isNull() & (sec_before_data_end <= MAX_NEXT_POLL_GAP_SEC), "in_progress")
        .when(F.col("next_polled_at").isNull() | (gap_to_next_poll_sec > MAX_NEXT_POLL_GAP_SEC), "censored")
        .when(close_on_last_sighting, "arrived")
        .otherwise("vanished"),
    ).withColumn(
        "actual_arrival_at",
        F.when(
            F.col("arrival_status") == "arrived",
            F.expr("timestamp_micros(unix_micros(last_seen_at) + (unix_micros(next_polled_at) - unix_micros(last_seen_at)) div 2)"),
        ),
    )

    visits = (
        add_time_features(visits.withColumn("event_at", F.coalesce("actual_arrival_at", "last_seen_at")), "event_at")
        .filter(F.col("arrival_status") != "in_progress")
        .select(*VISIT_COLUMNS)
    )
    return obs, visits


def build_train_outcomes(observations, visits):
    """Score every prediction made for a confirmed arrival. Same rules as the bus outcomes."""
    arrived = visits.filter(F.col("arrival_status") == "arrived").select("arrival_id", "actual_arrival_at")
    error = F.col("arrival_error_min")
    outcomes = (
        observations.join(arrived, "arrival_id")
        .withColumn("predicted_wait_min", F.greatest(F.lit(0.0), minutes_between("predicted_arrival_at", "polled_at")))
        .withColumn("actual_wait_min", minutes_between("actual_arrival_at", "polled_at"))
        .withColumn("arrival_error_min", minutes_between("actual_arrival_at", "predicted_arrival_at"))
        .filter(F.col("predicted_wait_min") < MAX_PREDICTED_WAIT_MIN)
        .withColumn("abs_error_min", F.abs(error))
        .withColumn("predicted_wait_bucket", bucket_label(F.col("predicted_wait_min"), WAIT_BUCKETS))
        .withColumn(
            "prediction_outcome",
            F.when(error < -TRAIN_ON_TIME_EARLY_TOLERANCE_MIN, "early")
            .when(error > TRAIN_ON_TIME_LATE_TOLERANCE_MIN, "late")
            .otherwise("on_time"),
        )
    )
    return add_time_features(outcomes, "polled_at").select(*OUTCOME_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Step 1: bronze -> silver.train_predictions (incremental stream)
if FULL_REFRESH:
    dbutils.fs.rm(CHECKPOINT_TRAIN_SILVER_PREDICTIONS, recurse=True)
    spark.sql(f"TRUNCATE TABLE {TBL_TRAIN_PREDICTIONS}")

rows_before = spark.table(TBL_TRAIN_PREDICTIONS).count()
query = (
    clean_train_predictions(spark.readStream.table(TBL_TRAIN_BRONZE_PREDICTIONS))
    .writeStream
    .option("checkpointLocation", CHECKPOINT_TRAIN_SILVER_PREDICTIONS)
    .trigger(availableNow=True)
    .toTable(TBL_TRAIN_PREDICTIONS)
)
query.awaitTermination()
print(f"Appended {spark.table(TBL_TRAIN_PREDICTIONS).count() - rows_before:,} rows to {TBL_TRAIN_PREDICTIONS}")

# COMMAND ----------

# DBTITLE 1,Step 2: rebuild arrivals and outcomes for the latest dates
start_date = reprocess_start_date(TBL_TRAIN_PREDICTIONS, FULL_REFRESH)
if start_date is None:
    dbutils.notebook.exit("No train predictions yet; nothing to build.")

# Read a little history before the window so visits that started just before midnight are complete.
read_from = f"{(start_date - timedelta(days=1)).isoformat()} {24 - LOOKBACK_BUFFER_HOURS:02d}:00:00"
preds = spark.table(TBL_TRAIN_PREDICTIONS).filter(
    (F.col("local_date") >= F.lit(start_date - timedelta(days=1)))
    & (F.col("polled_at") >= F.to_timestamp(F.lit(read_from)))
)

observations, visits = build_train_visits(preds)
overwrite_dates(visits, TBL_TRAIN_ARRIVALS, start_date)

# Score predictions against the arrivals just written (reading them back avoids recomputing the visits;
# serverless compute does not support .cache()).
saved_visits = spark.table(TBL_TRAIN_ARRIVALS).filter(F.col("local_date") >= F.lit(start_date - timedelta(days=1)))
overwrite_dates(build_train_outcomes(observations, saved_visits), TBL_TRAIN_OUTCOMES, start_date)
print(f"Rebuilt {TBL_TRAIN_ARRIVALS} and {TBL_TRAIN_OUTCOMES} from {start_date}")

# COMMAND ----------

# DBTITLE 1,Check: how did train visits end?
display(
    spark.table(TBL_TRAIN_ARRIVALS)
    .filter(F.col("local_date") >= F.lit(start_date))
    .groupBy("local_date", "arrival_status").count()
    .orderBy("local_date", "arrival_status")
)

# COMMAND ----------

# DBTITLE 1,Check: on-time rate by time of day (weekdays)
display(
    spark.table(TBL_TRAIN_OUTCOMES)
    .filter(F.col("day_type") == "Weekday")
    .groupBy("time_period")
    .agg(
        F.count("*").alias("predictions"),
        F.round(F.avg((F.col("prediction_outcome") == "on_time").cast("double")) * 100, 1).alias("on_time_pct"),
        F.round(F.avg("abs_error_min"), 2).alias("avg_minutes_off"),
    )
    .orderBy("time_period")
)

# COMMAND ----------

# DBTITLE 1,Table maintenance (compacts small files from frequent appends)
for table in [TBL_TRAIN_BRONZE_PREDICTIONS, TBL_TRAIN_PREDICTIONS]:
    spark.sql(f"OPTIMIZE {table}")