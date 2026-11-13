# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Train · Gold · Dashboard tables (job: hourly, after train_silver)
# MAGIC
# MAGIC The train twin of `gold`. Each table has the **same columns as its bus counterpart**, so a dashboard dataset
# MAGIC written for buses works for trains by changing the table name. The one exception is `bus_visits`, which is
# MAGIC `train_visits` here.
# MAGIC
# MAGIC Like the bus tables, these hold **counts, not percentages**: compute rates in the dashboard as
# MAGIC `SUM(on_time_predictions) / SUM(predictions)`.
# MAGIC
# MAGIC | Table | Bus counterpart | Answers |
# MAGIC |---|---|---|
# MAGIC | `gold.train_prediction_accuracy` | `gold.prediction_accuracy` | How often is the tracker right, by hour, platform, line and how far out the prediction was |
# MAGIC | `gold.train_prediction_error_distribution` | `gold.prediction_error_distribution` | When it's wrong, by how much (early vs late histogram) |
# MAGIC | `gold.train_arrival_quality` | `gold.arrival_quality` | How many train visits produced a usable arrival (data-quality view) |
# MAGIC | `gold.train_rider_predictions` | `gold.rider_predictions` | Rider dashboard: one row per prediction made 2-15 min out |
# MAGIC
# MAGIC For trains, `route_*` is the line, `stop_*` is the platform (one direction at one station), and `direction`
# MAGIC is CTA's platform description (e.g. "Service toward Loop").
# MAGIC
# MAGIC ### Dashboard layout
# MAGIC * **Bus page**: datasets on the bus gold tables (unchanged).
# MAGIC * **Train page**: copies of the same datasets, pointed at the `train_` tables.
# MAGIC * **Compare page**: one dataset that stacks both modes, e.g.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 'Bus' AS mode, * FROM cta_data.gold.prediction_accuracy
# MAGIC UNION ALL
# MAGIC SELECT 'Train' AS mode, * FROM cta_data.gold.train_prediction_accuracy
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Rebuild all history")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

# DBTITLE 1,Tables
gold_dims = """
  local_date            DATE,
  day_of_week           STRING,
  day_type              STRING,
  {extra}
  time_period           STRING,
  route_id              STRING,
  route_label           STRING,
  direction             STRING,
  stop_id               STRING,
  stop_label            STRING,
  area                  STRING"""

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_GOLD_ACCURACY} (
  {gold_dims.format(extra="hour_of_day INT, hour_label STRING,")},
  latitude              DOUBLE,
  longitude             DOUBLE,
  predicted_wait_bucket STRING,
  predictions           BIGINT COMMENT 'Predictions scored',
  on_time_predictions   BIGINT,
  early_predictions     BIGINT,
  late_predictions      BIGINT,
  sum_abs_error_min     DOUBLE COMMENT 'Divide by predictions for average minutes off',
  sum_error_min         DOUBLE COMMENT 'Divide by predictions for average bias (positive = late)'
)
CLUSTER BY (local_date)
COMMENT 'Train prediction accuracy, additive counts. Compute rates in the dashboard as SUM(x) / SUM(predictions).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_GOLD_ERROR_DIST} (
  {gold_dims.format(extra="")},
  predicted_wait_bucket STRING,
  error_min             INT    COMMENT 'Arrival error rounded to the minute, clipped to the histogram range',
  prediction_outcome    STRING,
  predictions           BIGINT
)
CLUSTER BY (local_date)
COMMENT 'Histogram of train arrival error (how far off the tracker was)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_GOLD_ARRIVAL_QUALITY} (
  {gold_dims.format(extra="")},
  train_visits          BIGINT COMMENT 'Train visits to the platform we saw predicted',
  arrived_visits        BIGINT COMMENT 'Visits with a confirmed arrival (used for accuracy)',
  vanished_visits       BIGINT COMMENT 'Train dropped out of predictions while still minutes away',
  censored_visits       BIGINT COMMENT 'Polling gap: arrival happened while we were not watching'
)
CLUSTER BY (local_date)
COMMENT 'How many train visits produced a usable arrival. Data-quality view for the dashboard.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL_TRAIN_GOLD_RIDER} (
  local_date            DATE,
  day_type              STRING,
  time_period           STRING,
  hour_of_day           INT,
  hour_label            STRING,
  route_id              STRING,
  route_label           STRING,
  stop_id               STRING,
  stop_label            STRING,
  area                  STRING,
  latitude              DOUBLE,
  longitude             DOUBLE,
  predicted_wait_bucket STRING,
  on_time               DOUBLE COMMENT '1 if the train came within the on-time window, else 0. AVG(on_time) = on-time rate',
  is_early              DOUBLE COMMENT '1 if the train came more than the early tolerance before the prediction',
  is_late               DOUBLE COMMENT '1 if the train came more than the late tolerance after the prediction',
  abs_error_min         DOUBLE COMMENT 'Minutes off, either direction. AVG(abs_error_min) = typical minutes off'
)
CLUSTER BY (local_date, stop_id)
COMMENT 'Train rider dashboard table: one row per prediction made 2-15 min out, labeled. Every rate is a plain AVG, so any filter gives an exact answer.'
""")
print(f"Ready: {TBL_TRAIN_GOLD_ACCURACY}, {TBL_TRAIN_GOLD_ERROR_DIST}, {TBL_TRAIN_GOLD_ARRIVAL_QUALITY}, {TBL_TRAIN_GOLD_RIDER}")

# COMMAND ----------

# DBTITLE 1,Aggregation functions
SHARED_DIMS = ["local_date", "day_of_week", "day_type", "time_period",
               "route_id", "route_label", "direction", "stop_id", "stop_label", "area"]

ACCURACY_DIMS = ["local_date", "day_of_week", "day_type", "hour_of_day", "hour_label", "time_period",
                 "route_id", "route_label", "direction", "stop_id", "stop_label", "area",
                 "latitude", "longitude", "predicted_wait_bucket"]


def add_labels(df, stops, routes):
    """Attach display labels and coordinates. Unknown platforms/lines fall back to their names or IDs."""
    return (
        df.join(stops, "stop_id", "left")
        .join(routes, "route_id", "left")
        .withColumn("stop_label", F.coalesce("stop_label", "stop_name", "stop_id"))
        .withColumn("route_label", F.coalesce("route_label", "route_id"))
        .withColumn("area", F.coalesce("area", F.lit("Other")))
    )


def count_where(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("bigint")


def build_accuracy(outcomes):
    outcome = F.col("prediction_outcome")
    return outcomes.groupBy(*ACCURACY_DIMS).agg(
        F.count("*").alias("predictions"),
        count_where(outcome == "on_time").alias("on_time_predictions"),
        count_where(outcome == "early").alias("early_predictions"),
        count_where(outcome == "late").alias("late_predictions"),
        F.sum("abs_error_min").alias("sum_abs_error_min"),
        F.sum("arrival_error_min").alias("sum_error_min"),
    )


def build_error_distribution(outcomes):
    clipped = F.least(F.greatest(F.round("arrival_error_min"), F.lit(float(ERROR_BIN_MIN))), F.lit(float(ERROR_BIN_MAX)))
    return (
        outcomes.withColumn("error_min", clipped.cast("int"))
        .groupBy(*SHARED_DIMS, "predicted_wait_bucket", "error_min", "prediction_outcome")
        .agg(F.count("*").alias("predictions"))
    )


def build_arrival_quality(arrivals):
    status = F.col("arrival_status")
    return arrivals.groupBy(*SHARED_DIMS).agg(
        F.count("*").alias("train_visits"),
        count_where(status == "arrived").alias("arrived_visits"),
        count_where(status == "vanished").alias("vanished_visits"),
        count_where(status == "censored").alias("censored_visits"),
    )


def build_rider_predictions(outcomes):
    """One row per prediction in the rider window, with 0/1 flags so dashboard rates are plain averages."""
    outcome = F.col("prediction_outcome")
    return (
        outcomes.filter(F.col("predicted_wait_bucket").isin(RIDER_WAIT_BUCKETS))
        .select(
            "local_date", "day_type", "time_period", "hour_of_day", "hour_label",
            "route_id", "route_label", "stop_id", "stop_label", "area", "latitude", "longitude",
            "predicted_wait_bucket",
            (outcome == "on_time").cast("double").alias("on_time"),
            (outcome == "early").cast("double").alias("is_early"),
            (outcome == "late").cast("double").alias("is_late"),
            "abs_error_min",
        )
    )

# COMMAND ----------

# DBTITLE 1,Build and write
start_date = reprocess_start_date(TBL_TRAIN_ARRIVALS, FULL_REFRESH)
if start_date is None:
    dbutils.notebook.exit("No silver train arrivals yet; nothing to build.")

stops = spark.table(TBL_TRAIN_STOPS).select("stop_id", "stop_label", "area", "latitude", "longitude")
routes = spark.table(TBL_TRAIN_ROUTES).select("route_id", "route_label")
in_window = F.col("local_date") >= F.lit(start_date)

outcomes = add_labels(spark.table(TBL_TRAIN_OUTCOMES).filter(in_window), stops, routes)
arrivals = add_labels(spark.table(TBL_TRAIN_ARRIVALS).filter(in_window), stops, routes)

overwrite_dates(build_accuracy(outcomes), TBL_TRAIN_GOLD_ACCURACY, start_date)
overwrite_dates(build_error_distribution(outcomes), TBL_TRAIN_GOLD_ERROR_DIST, start_date)
overwrite_dates(build_arrival_quality(arrivals), TBL_TRAIN_GOLD_ARRIVAL_QUALITY, start_date)
overwrite_dates(build_rider_predictions(outcomes), TBL_TRAIN_GOLD_RIDER, start_date)
print(f"Train gold rebuilt from {start_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: the headline numbers the Train page will show

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   day_type,
# MAGIC   time_period,
# MAGIC   SUM(predictions)                                              AS predictions,
# MAGIC   ROUND(100 * SUM(on_time_predictions) / SUM(predictions), 1)   AS on_time_pct,
# MAGIC   ROUND(100 * SUM(early_predictions) / SUM(predictions), 1)     AS early_pct,
# MAGIC   ROUND(SUM(sum_abs_error_min) / SUM(predictions), 2)           AS avg_minutes_off
# MAGIC FROM cta_data.gold.train_prediction_accuracy
# MAGIC WHERE predicted_wait_bucket = '05-10 min'
# MAGIC GROUP BY day_type, time_period
# MAGIC ORDER BY day_type, time_period

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: bus vs train, side by side (what the Compare page will show)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   mode,
# MAGIC   time_period,
# MAGIC   SUM(predictions)                                              AS predictions,
# MAGIC   ROUND(100 * SUM(on_time_predictions) / SUM(predictions), 1)   AS on_time_pct,
# MAGIC   ROUND(SUM(sum_abs_error_min) / SUM(predictions), 2)           AS avg_minutes_off
# MAGIC FROM (
# MAGIC   SELECT 'Bus' AS mode, * FROM cta_data.gold.prediction_accuracy
# MAGIC   UNION ALL
# MAGIC   SELECT 'Train' AS mode, * FROM cta_data.gold.train_prediction_accuracy
# MAGIC )
# MAGIC WHERE predicted_wait_bucket = '05-10 min' AND day_type = 'Weekday'
# MAGIC GROUP BY mode, time_period
# MAGIC ORDER BY time_period, mode