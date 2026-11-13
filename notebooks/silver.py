# Databricks notebook source
# MAGIC %md
# MAGIC # Silver · Clean predictions and score them against real arrivals (job: hourly)
# MAGIC
# MAGIC | Table | Grain | How it's built |
# MAGIC |---|---|---|
# MAGIC | `silver.bus_predictions` | one prediction per poll | Structured Streaming from bronze (`availableNow`), typed and renamed |
# MAGIC | `silver.bus_arrivals` | one bus visit to a stop | rebuilt for the latest dates with `replaceWhere` |
# MAGIC | `silver.prediction_outcomes` | one prediction scored against the real arrival | same |
# MAGIC
# MAGIC ### How an arrival is detected
# MAGIC CTA never reports actual arrivals, so we infer them. Each poll lists the buses predicted for each stop.
# MAGIC A **visit** is one vehicle on one trip approaching one stop. When the bus stops appearing in that stop's
# MAGIC predictions, it has served the stop. The visit gets one of three statuses:
# MAGIC
# MAGIC * **arrived**: the next poll of the stop came within `MAX_NEXT_POLL_GAP_SEC` and the bus was within
# MAGIC   `ARRIVAL_MAX_DISTANCE_FT` (or showed DUE) on its last sighting. Arrival time = midpoint between the two polls.
# MAGIC * **vanished**: the bus dropped out while still far away (detour, cancellation, reassignment). Not scored.
# MAGIC * **censored**: polling stopped or had a gap, so we never saw it arrive. Not scored.
# MAGIC
# MAGIC Visits still in progress at the latest poll are skipped and picked up next run.
# MAGIC Only `arrived` visits feed the accuracy numbers.

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
CREATE TABLE IF NOT EXISTS {TBL_PREDICTIONS} (
  poll_id                 STRING,
  polled_at               TIMESTAMP COMMENT 'When we called the API (the moment a rider would have looked)',
  prediction_generated_at TIMESTAMP COMMENT 'CTA tmstmp: when CTA produced the prediction',
  predicted_arrival_at    TIMESTAMP COMMENT 'CTA prdtm: when CTA said the bus would arrive',
  prediction_type         STRING    COMMENT 'arrival or departure (departures are terminal stops)',
  stop_id                 STRING,
  stop_name               STRING,
  vehicle_id              STRING,
  route_id                STRING,
  direction               STRING,
  destination             STRING,
  distance_to_stop_ft     INT       COMMENT 'Feet the bus still had to travel to the stop',
  countdown_label         STRING    COMMENT 'What the sign showed: minutes, DUE or DLY',
  countdown_min           INT       COMMENT 'Countdown in minutes (DUE = 0, DLY = NULL)',
  is_delayed              BOOLEAN,
  trip_id                 STRING    COMMENT 'CTA tatripid; one bus trip',
  block_id                STRING    COMMENT 'CTA tablockid; a vehicle work assignment',
  local_date              DATE      COMMENT 'Chicago date of polled_at',
  _ingested_at            TIMESTAMP
)
CLUSTER BY (local_date, stop_id)
COMMENT 'One row per prediction per poll, typed and renamed from bronze'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_ARRIVALS} (
  arrival_id          STRING    COMMENT 'Hash of vehicle, stop, trip and first sighting',
  vehicle_id          STRING,
  trip_id             STRING,
  route_id            STRING,
  direction           STRING,
  destination         STRING,
  stop_id             STRING,
  stop_name           STRING,
  first_seen_at       TIMESTAMP COMMENT 'First poll that predicted this bus at this stop',
  last_seen_at        TIMESTAMP COMMENT 'Last poll that still predicted it',
  observation_count   INT       COMMENT 'Polls in which the bus was predicted for this stop',
  last_distance_ft    INT       COMMENT 'Distance to the stop on the last sighting',
  closest_distance_ft INT,
  next_polled_at      TIMESTAMP COMMENT 'Next poll of this stop after the last sighting',
  arrival_status      STRING    COMMENT 'arrived, vanished (dropped out far from the stop) or censored (polling gap, arrival not observed)',
  actual_arrival_at   TIMESTAMP COMMENT 'Midpoint between last sighting and next poll; only when arrival_status = arrived',
  {time_columns}
)
CLUSTER BY (local_date)
COMMENT 'One row per bus visit to a stop. Time fields use the arrival time (or last sighting when not arrived).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_OUTCOMES} (
  arrival_id            STRING,
  poll_id               STRING,
  polled_at             TIMESTAMP,
  route_id              STRING,
  direction             STRING,
  stop_id               STRING,
  stop_name             STRING,
  vehicle_id            STRING,
  trip_id               STRING,
  predicted_arrival_at  TIMESTAMP,
  actual_arrival_at     TIMESTAMP,
  predicted_wait_min    DOUBLE  COMMENT 'What the tracker said: minutes from polled_at to predicted arrival',
  actual_wait_min       DOUBLE  COMMENT 'Minutes from polled_at to the actual arrival',
  arrival_error_min     DOUBLE  COMMENT 'actual minus predicted; positive = bus came later than promised',
  abs_error_min         DOUBLE,
  predicted_wait_bucket STRING  COMMENT 'e.g. 05-10 min',
  prediction_outcome    STRING  COMMENT 'early, on_time or late (tolerances in _common)',
  is_delayed            BOOLEAN,
  {time_columns}
)
CLUSTER BY (local_date)
COMMENT 'Every prediction for a confirmed arrival, scored against what actually happened. Time fields use polled_at.'
""")
print(f"Ready: {TBL_PREDICTIONS}, {TBL_ARRIVALS}, {TBL_OUTCOMES}")

# COMMAND ----------

# DBTITLE 1,Transform functions
TIME_COLUMNS = ["local_date", "hour_of_day", "hour_label", "day_of_week", "day_type", "time_period"]

VISIT_COLUMNS = [
    "arrival_id", "vehicle_id", "trip_id", "route_id", "direction", "destination", "stop_id", "stop_name",
    "first_seen_at", "last_seen_at", "observation_count", "last_distance_ft", "closest_distance_ft",
    "next_polled_at", "arrival_status", "actual_arrival_at", *TIME_COLUMNS,
]

OUTCOME_COLUMNS = [
    "arrival_id", "poll_id", "polled_at", "route_id", "direction", "stop_id", "stop_name", "vehicle_id", "trip_id",
    "predicted_arrival_at", "actual_arrival_at", "predicted_wait_min", "actual_wait_min", "arrival_error_min",
    "abs_error_min", "predicted_wait_bucket", "prediction_outcome", "is_delayed", *TIME_COLUMNS,
]


def clean_predictions(bronze):
    """Bronze (CTA names, all strings) -> typed, renamed silver columns. Stateless, so it works on a stream."""
    countdown = clean_str("prdctdn")
    return bronze.select(
        "poll_id",
        "polled_at",
        parse_cta_time("tmstmp").alias("prediction_generated_at"),
        parse_cta_time("prdtm").alias("predicted_arrival_at"),
        F.when(F.col("typ") == "A", "arrival").when(F.col("typ") == "D", "departure").alias("prediction_type"),
        clean_str("stpid").alias("stop_id"),
        clean_str("stpnm").alias("stop_name"),
        clean_str("vid").alias("vehicle_id"),
        clean_str("rt").alias("route_id"),
        clean_str("rtdir").alias("direction"),
        clean_str("des").alias("destination"),
        F.expr("try_cast(dstp AS INT)").alias("distance_to_stop_ft"),
        countdown.alias("countdown_label"),
        F.when(countdown == "DUE", F.lit(0)).otherwise(F.expr("try_cast(prdctdn AS INT)")).alias("countdown_min"),
        F.coalesce(F.lower(clean_str("dly")) == "true", F.lit(False)).alias("is_delayed"),
        clean_str("tatripid").alias("trip_id"),
        clean_str("tablockid").alias("block_id"),
        F.to_date("polled_at").alias("local_date"),
        "_ingested_at",
    )


def build_visits(preds):
    """Group arrival predictions into bus visits and decide how each one ended.

    Returns (observations, visits):
      observations: each arrival prediction tagged with its arrival_id
      visits:       one row per finished visit, with arrival_status and actual_arrival_at
    """
    # Every poll in which a stop returned any prediction: tells us when we next looked at the stop.
    stop_polls = (
        preds.select("stop_id", F.col("polled_at").alias("last_seen_at")).distinct()
        .withColumn("next_polled_at", F.lead("last_seen_at").over(Window.partitionBy("stop_id").orderBy("last_seen_at")))
    )
    data_end = preds.agg(F.max("polled_at").alias("data_end_at"))

    obs = (
        preds.filter(
            (F.col("prediction_type").isin("arrival", "departure"))
            & F.col("vehicle_id").isNotNull()
            & F.col("stop_id").isNotNull()
            & F.col("predicted_arrival_at").isNotNull()
        )
        .withColumn("trip_key", F.coalesce("trip_id", F.lit("?")))
        .dropDuplicates(["poll_id", "vehicle_id", "stop_id", "trip_key"])
    )

    # One visit = vehicle x trip x stop. If a trip ID is missing, a long gap also starts a new visit.
    w_visit = Window.partitionBy("vehicle_id", "stop_id", "trip_key").orderBy("polled_at")
    obs = (
        obs.withColumn("prev_polled_at", F.lag("polled_at").over(w_visit))
        .withColumn(
            "starts_visit",
            (F.col("prev_polled_at").isNull()
             | (minutes_between("polled_at", "prev_polled_at") > VISIT_SPLIT_GAP_MIN)).cast("int"),
        )
        .withColumn("visit_seq", F.sum("starts_visit").over(w_visit.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn(
            "visit_start_at",
            F.min("polled_at").over(Window.partitionBy("vehicle_id", "stop_id", "trip_key", "visit_seq")),
        )
        .withColumn(
            "arrival_id",
            F.substring(F.sha2(F.concat_ws(
                "|", "vehicle_id", "stop_id", "trip_key", F.expr("cast(unix_micros(visit_start_at) as string)")
            ), 256), 1, 20),
        )
    )

    last_sighting = (
        obs.withColumn("rn", F.row_number().over(Window.partitionBy("arrival_id").orderBy(F.desc("polled_at"))))
        .filter("rn = 1")
        .select(
            "arrival_id", "route_id", "direction", "destination", "stop_name", "trip_id",
            F.col("distance_to_stop_ft").alias("last_distance_ft"),
            F.col("countdown_label").alias("last_countdown_label"),
        )
    )

    visits = (
        obs.groupBy("arrival_id", "vehicle_id", "stop_id")
        .agg(
            F.min("polled_at").alias("first_seen_at"),
            F.max("polled_at").alias("last_seen_at"),
            F.count("*").cast("int").alias("observation_count"),
            F.min("distance_to_stop_ft").alias("closest_distance_ft"),
        )
        .join(last_sighting, "arrival_id")
        .join(stop_polls, ["stop_id", "last_seen_at"], "left")
        .crossJoin(F.broadcast(data_end))
    )

    gap_to_next_poll_sec = F.expr("(unix_micros(next_polled_at) - unix_micros(last_seen_at)) / 1.0E6")
    sec_before_data_end = F.expr("(unix_micros(data_end_at) - unix_micros(last_seen_at)) / 1.0E6")
    close_on_last_sighting = (F.col("last_distance_ft") <= ARRIVAL_MAX_DISTANCE_FT) | (F.col("last_countdown_label") == "DUE")

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


def build_outcomes(observations, visits):
    """Score every prediction made for a confirmed arrival."""
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
            F.when(error < -ON_TIME_EARLY_TOLERANCE_MIN, "early")
            .when(error > ON_TIME_LATE_TOLERANCE_MIN, "late")
            .otherwise("on_time"),
        )
    )
    return add_time_features(outcomes, "polled_at").select(*OUTCOME_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Step 1: bronze -> silver.bus_predictions (incremental stream)
if FULL_REFRESH:
    dbutils.fs.rm(CHECKPOINT_SILVER_PREDICTIONS, recurse=True)
    spark.sql(f"TRUNCATE TABLE {TBL_PREDICTIONS}")

rows_before = spark.table(TBL_PREDICTIONS).count()
query = (
    clean_predictions(spark.readStream.table(TBL_BRONZE_PREDICTIONS))
    .writeStream
    .option("checkpointLocation", CHECKPOINT_SILVER_PREDICTIONS)
    .trigger(availableNow=True)
    .toTable(TBL_PREDICTIONS)
)
query.awaitTermination()
print(f"Appended {spark.table(TBL_PREDICTIONS).count() - rows_before:,} rows to {TBL_PREDICTIONS}")

# COMMAND ----------

# DBTITLE 1,Step 2: rebuild arrivals and outcomes for the latest dates
start_date = reprocess_start_date(TBL_PREDICTIONS, FULL_REFRESH)
if start_date is None:
    dbutils.notebook.exit("No predictions yet; nothing to build.")

# Read a little history before the window so visits that started just before midnight are complete.
read_from = f"{(start_date - timedelta(days=1)).isoformat()} {24 - LOOKBACK_BUFFER_HOURS:02d}:00:00"
preds = spark.table(TBL_PREDICTIONS).filter(
    (F.col("local_date") >= F.lit(start_date - timedelta(days=1)))
    & (F.col("polled_at") >= F.to_timestamp(F.lit(read_from)))
)

observations, visits = build_visits(preds)
overwrite_dates(visits, TBL_ARRIVALS, start_date)

# Score predictions against the arrivals just written (reading them back avoids recomputing the visits;
# serverless compute does not support .cache()).
saved_visits = spark.table(TBL_ARRIVALS).filter(F.col("local_date") >= F.lit(start_date - timedelta(days=1)))
overwrite_dates(build_outcomes(observations, saved_visits), TBL_OUTCOMES, start_date)
print(f"Rebuilt {TBL_ARRIVALS} and {TBL_OUTCOMES} from {start_date}")

# COMMAND ----------

# DBTITLE 1,Check: how did bus visits end?
display(
    spark.table(TBL_ARRIVALS)
    .filter(F.col("local_date") >= F.lit(start_date))
    .groupBy("local_date", "arrival_status").count()
    .orderBy("local_date", "arrival_status")
)

# COMMAND ----------

# DBTITLE 1,Check: on-time rate by time of day (weekdays)
display(
    spark.table(TBL_OUTCOMES)
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
for table in [TBL_BRONZE_PREDICTIONS, TBL_PREDICTIONS]:
    spark.sql(f"OPTIMIZE {table}")