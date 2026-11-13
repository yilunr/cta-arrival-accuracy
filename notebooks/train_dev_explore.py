# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Train · Dev · Explore and check the train pipeline
# MAGIC
# MAGIC The train twin of `dev_explore`. Read-only scratch notebook: nothing here writes to any table,
# MAGIC so run any cell, any time. Not part of a job.
# MAGIC
# MAGIC | Section | Use it to |
# MAGIC |---|---|
# MAGIC | 1. Pipeline health | See if the jobs are running, how fresh the data is, and spot polling gaps |
# MAGIC | 2. Table overview | Row counts and date ranges for every layer, and the platforms being tracked |
# MAGIC | 3. Raw data | Look at the latest poll exactly as CTA sent it |
# MAGIC | 4. Arrival detection | See how train visits ended, check the cutoff, and trace one visit poll by poll |
# MAGIC | 5. Results | The project question: accuracy by time of day, platform and line |
# MAGIC
# MAGIC Use the widgets at the top to narrow the date range or focus on one platform or line.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Filters
dbutils.widgets.text("days_back", "7", "Days back")
dbutils.widgets.text("stop_id", "", "Platform stop ID (blank = all)")
dbutils.widgets.text("route_id", "", "Line code, e.g. Brn (blank = all)")
dbutils.widgets.dropdown("wait_bucket", "05-10 min", [b[2] for b in WAIT_BUCKETS] + ["all"], "Tracker said")

DAYS_BACK = int(dbutils.widgets.get("days_back"))
STOP_ID = dbutils.widgets.get("stop_id").strip()
ROUTE_ID = dbutils.widgets.get("route_id").strip()
WAIT_BUCKET = dbutils.widgets.get("wait_bucket")

# Reusable SQL filter snippets
DATE_FILTER = f"local_date >= current_date() - {DAYS_BACK}"
STOP_FILTER = f"stop_id = '{STOP_ID}'" if STOP_ID else "true"
ROUTE_FILTER = f"route_id = '{ROUTE_ID}'" if ROUTE_ID else "true"
BUCKET_FILTER = f"predicted_wait_bucket = '{WAIT_BUCKET}'" if WAIT_BUCKET != "all" else "true"
WHERE = f"{DATE_FILTER} AND {STOP_FILTER} AND {ROUTE_FILTER}"

print(f"Last {DAYS_BACK} days | platform: {STOP_ID or 'all'} | line: {ROUTE_ID or 'all'} | tracker said: {WAIT_BUCKET}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline health

# COMMAND ----------

# DBTITLE 1,Freshness: how old is the newest data in each layer?
display(spark.sql(f"""
SELECT 'bronze.train_predictions_raw' AS table_name, max(polled_at) AS newest,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(polled_at))) / 60, 1) AS minutes_ago
FROM {TBL_TRAIN_BRONZE_PREDICTIONS}
UNION ALL
SELECT 'silver.train_predictions', max(polled_at),
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(polled_at))) / 60, 1)
FROM {TBL_TRAIN_PREDICTIONS}
UNION ALL
SELECT 'silver.train_arrivals', max(last_seen_at),
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(last_seen_at))) / 60, 1)
FROM {TBL_TRAIN_ARRIVALS}
"""))
# Healthy: bronze < 5 min old, silver < ~70 min (hourly job), arrivals a bit older than silver.

# COMMAND ----------

# DBTITLE 1,Ingestion runs per hour (want ~60/hour at 1-minute polling, ~30 at 2-minute)
display(spark.sql(f"""
SELECT date_trunc('hour', started_at)        AS hour,
       count(*)                              AS runs,
       count_if(status = 'partial')          AS partial_runs,
       count_if(status = 'failed')           AS failed_runs,
       sum(api_calls)                        AS api_calls,
       sum(predictions_landed)               AS predictions_landed
FROM {TBL_TRAIN_INGESTION_RUNS}
WHERE started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY hour DESC
"""))

# COMMAND ----------

# DBTITLE 1,API calls per day vs the daily key limit
display(spark.sql(f"""
SELECT to_date(started_at) AS day,
       sum(api_calls)      AS api_calls,
       round(100 * sum(api_calls) / {TRAIN_DAILY_CALL_LIMIT}, 1) AS pct_of_daily_limit
FROM {TBL_TRAIN_INGESTION_RUNS}
WHERE started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY day DESC
"""))

# COMMAND ----------

# DBTITLE 1,Recent problems: runs that weren't clean
display(spark.sql(f"""
SELECT started_at, status, api_calls, failed_api_calls, predictions_landed, api_messages
FROM {TBL_TRAIN_INGESTION_RUNS}
WHERE status <> 'success' AND started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
ORDER BY started_at DESC
LIMIT 50
"""))
# api_messages are "code: message" straight from CTA. 103/108 = a station or platform ID CTA no longer accepts
# (re-run train_bronze with rediscover = true). 105 = too many stations per call (lower MAX_STATIONS_PER_CALL).

# COMMAND ----------

# DBTITLE 1,Polling gaps: minutes between consecutive polls (anything over 3 breaks arrival detection)
display(spark.sql(f"""
WITH polls AS (
  SELECT DISTINCT polled_at FROM {TBL_TRAIN_PREDICTIONS} WHERE {DATE_FILTER}
)
SELECT prev_polled_at AS gap_started, polled_at AS gap_ended,
       round((unix_timestamp(polled_at) - unix_timestamp(prev_polled_at)) / 60, 1) AS gap_min
FROM (SELECT polled_at, lag(polled_at) OVER (ORDER BY polled_at) AS prev_polled_at FROM polls)
WHERE (unix_timestamp(polled_at) - unix_timestamp(prev_polled_at)) > 180
ORDER BY gap_started DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table overview

# COMMAND ----------

# DBTITLE 1,Row counts and date ranges, every layer
tables = [
    ("bronze", TBL_TRAIN_BRONZE_PREDICTIONS, "to_date(polled_at)"),
    ("silver", TBL_TRAIN_PREDICTIONS, "local_date"),
    ("silver", TBL_TRAIN_ARRIVALS, "local_date"),
    ("silver", TBL_TRAIN_OUTCOMES, "local_date"),
    ("gold", TBL_TRAIN_GOLD_ACCURACY, "local_date"),
    ("gold", TBL_TRAIN_GOLD_ERROR_DIST, "local_date"),
    ("gold", TBL_TRAIN_GOLD_ARRIVAL_QUALITY, "local_date"),
    ("gold", TBL_TRAIN_GOLD_RIDER, "local_date"),
]
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{layer}' AS layer, '{t}' AS table_name, count(*) AS row_count, "
    f"min({d}) AS first_date, max({d}) AS last_date FROM {t}"
    for layer, t, d in tables
)))

# COMMAND ----------

# DBTITLE 1,Tracked platforms (from discovery)
display(spark.sql(f"""
SELECT station_id, stop_id, stop_label, direction, served_by_routes, area, is_active, last_discovered_at
FROM {TBL_TRAIN_STOPS}
ORDER BY is_active DESC, area, stop_name, stop_id
"""))
# First run: check the Loop list looks right (about 17 stations, two platforms each). To widen or narrow it,
# edit config.train_stop_discovery_rules and re-run train_bronze with rediscover = true.

# COMMAND ----------

# DBTITLE 1,Platforms that returned predictions but aren't in the tracked list (should be empty)
display(spark.sql(f"""
SELECT p.station_id, p.stop_id, p.stop_name, p.direction, count(*) AS predictions
FROM {TBL_TRAIN_PREDICTIONS} p
LEFT ANTI JOIN {TBL_TRAIN_STOPS} s ON p.stop_id = s.stop_id
WHERE {DATE_FILTER}
GROUP BY ALL
ORDER BY predictions DESC
"""))
# Rows here still flow through to gold, with area = 'Other' and the station name as the label.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Raw data

# COMMAND ----------

# DBTITLE 1,Latest poll, exactly as CTA returned it
display(spark.sql(f"""
SELECT polled_at, staId, stpId, staNm, stpDe, rt, rn, destNm, prdt, arrT, isApp, isSch, isDly, isFlt
FROM {TBL_TRAIN_BRONZE_PREDICTIONS}
WHERE polled_at = (SELECT max(polled_at) FROM {TBL_TRAIN_BRONZE_PREDICTIONS})
  AND {STOP_FILTER.replace('stop_id', 'stpId')}
  AND {ROUTE_FILTER.replace('route_id', 'rt')}
ORDER BY staNm, stpId, arrT
"""))

# COMMAND ----------

# DBTITLE 1,Fields CTA sent that aren't in the schema (should be empty or rare)
display(spark.sql(f"""
SELECT _rescued_data, count(*) AS rows
FROM {TBL_TRAIN_BRONZE_PREDICTIONS}
WHERE _rescued_data IS NOT NULL AND polled_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY rows DESC
LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Timestamps that failed to parse (should be zero)
display(spark.sql(f"""
SELECT local_date,
       count(*)                                      AS predictions,
       count_if(predicted_arrival_at IS NULL)        AS unparsed_arrival_times,
       count_if(prediction_generated_at IS NULL)     AS unparsed_generated_times
FROM {TBL_TRAIN_PREDICTIONS}
WHERE {WHERE}
GROUP BY ALL
ORDER BY local_date DESC
"""))
# Non-zero means CTA changed its time format: look at prdt / arrT in the raw cell above and extend parse_train_time.

# COMMAND ----------

# DBTITLE 1,Live vs scheduled predictions by line
display(spark.sql(f"""
SELECT route_id,
       count(*)                                               AS predictions,
       round(100 * count_if(is_scheduled) / count(*), 1)      AS scheduled_pct,
       round(100 * count_if(is_delayed)   / count(*), 1)      AS delayed_pct,
       round(100 * count_if(is_fault)     / count(*), 1)      AS fault_pct
FROM {TBL_TRAIN_PREDICTIONS}
WHERE {WHERE}
GROUP BY ALL
ORDER BY predictions DESC
"""))
# Scheduled predictions (timetable, no live train yet) are not scored unless TRAIN_SCORE_SCHEDULED is on.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Arrival detection

# COMMAND ----------

# DBTITLE 1,How did train visits end, per day?
display(spark.sql(f"""
SELECT local_date,
       count(*)                                                         AS visits,
       round(100 * count_if(arrival_status = 'arrived')  / count(*), 1) AS arrived_pct,
       round(100 * count_if(arrival_status = 'vanished') / count(*), 1) AS vanished_pct,
       round(100 * count_if(arrival_status = 'censored') / count(*), 1) AS censored_pct
FROM {TBL_TRAIN_ARRIVALS}
WHERE {WHERE}
GROUP BY ALL
ORDER BY local_date DESC
"""))
# Healthy with continuous polling: mostly arrived. High censored = polling gaps. High vanished = check the next cell.

# COMMAND ----------

# DBTITLE 1,Predicted wait on the last sighting before the train dropped out (is the 3-minute cutoff right?)
display(spark.sql(f"""
SELECT least(floor(last_predicted_wait_min * 2) / 2, 10) AS last_predicted_wait_min_bucket,
       was_approaching,
       arrival_status,
       count(*) AS visits
FROM {TBL_TRAIN_ARRIVALS}
WHERE {WHERE} AND arrival_status <> 'censored'
GROUP BY ALL
ORDER BY last_predicted_wait_min_bucket, was_approaching
"""))
# Most visits should drop out within a minute or two of the platform. A big bump just above the cutoff would mean
# TRAIN_ARRIVAL_MAX_LAST_WAIT_MIN in _common is too strict for your polling interval.

# COMMAND ----------

# DBTITLE 1,Run numbers that came back to the same platform (checks the visit split)
display(spark.sql(f"""
SELECT local_date, route_id, run_number, stop_name, stop_id, count(*) AS visits,
       min(first_seen_at) AS first_visit_started, max(first_seen_at) AS last_visit_started
FROM {TBL_TRAIN_ARRIVALS}
WHERE {WHERE}
GROUP BY ALL
HAVING count(*) > 1
ORDER BY visits DESC
LIMIT 50
"""))
# A handful of visits per run per day is normal (round trips). Dozens for one run would mean visits are being
# split too eagerly: raise VISIT_SPLIT_GAP_MIN.

# COMMAND ----------

# DBTITLE 1,Trace one arrived visit poll by poll
one = spark.sql(f"""
SELECT arrival_id FROM {TBL_TRAIN_ARRIVALS}
WHERE {WHERE} AND arrival_status = 'arrived' AND observation_count >= 5
ORDER BY last_seen_at DESC LIMIT 1
""").first()

if one is None:
    print("No arrived visits with 5+ observations in this filter yet.")
else:
    display(spark.sql(f"""
    SELECT o.polled_at, o.predicted_arrival_at, a.actual_arrival_at,
           round(o.predicted_wait_min, 1) AS tracker_said_min,
           round(o.actual_wait_min, 1)    AS actual_wait_min,
           round(o.arrival_error_min, 1)  AS error_min,
           o.prediction_outcome, a.route_id, a.stop_name, a.direction, a.run_number
    FROM {TBL_TRAIN_OUTCOMES} o JOIN {TBL_TRAIN_ARRIVALS} a USING (arrival_id)
    WHERE arrival_id = '{one.arrival_id}'
    ORDER BY o.polled_at
    """))
# Each row is one moment a rider could have checked the app. Error should shrink as the train gets closer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Results

# COMMAND ----------

# DBTITLE 1,Headline: on-time rate by time of day, weekday vs weekend
display(spark.sql(f"""
SELECT time_period, day_type,
       sum(predictions)                                              AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)   AS on_time_pct,
       round(100 * sum(early_predictions)   / sum(predictions), 1)   AS early_pct,
       round(100 * sum(late_predictions)    / sum(predictions), 1)   AS late_pct,
       round(sum(sum_abs_error_min) / sum(predictions), 2)           AS avg_minutes_off
FROM {TBL_TRAIN_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
ORDER BY day_type, time_period
"""))

# COMMAND ----------

# DBTITLE 1,Heatmap: on-time % by hour x how far out the prediction was
heat = spark.sql(f"""
SELECT hour_label, predicted_wait_bucket,
       round(100 * sum(on_time_predictions) / sum(predictions), 0) AS on_time_pct
FROM {TBL_TRAIN_GOLD_ACCURACY}
WHERE {WHERE}
GROUP BY ALL
""")
display(heat.groupBy("hour_label").pivot("predicted_wait_bucket").agg(F.first("on_time_pct")).orderBy("hour_label"))
# Read across a column: accuracy through the day. Read across a row: how accuracy fades the further out the prediction is.

# COMMAND ----------

# DBTITLE 1,Platforms ranked by reliability (min 50 predictions)
display(spark.sql(f"""
SELECT stop_label, area,
       sum(predictions)                                             AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)  AS on_time_pct,
       round(100 * sum(early_predictions)   / sum(predictions), 1)  AS early_pct,
       round(sum(sum_abs_error_min) / sum(predictions), 2)          AS avg_minutes_off
FROM {TBL_TRAIN_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
HAVING sum(predictions) >= 50
ORDER BY on_time_pct DESC
"""))

# COMMAND ----------

# DBTITLE 1,Lines ranked by reliability (min 50 predictions)
display(spark.sql(f"""
SELECT route_label,
       sum(predictions)                                             AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)  AS on_time_pct,
       round(sum(sum_error_min) / sum(predictions), 2)              AS avg_bias_min,
       round(sum(sum_abs_error_min) / sum(predictions), 2)          AS avg_minutes_off
FROM {TBL_TRAIN_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
HAVING sum(predictions) >= 50
ORDER BY on_time_pct DESC
"""))
# avg_bias_min > 0 means trains on that line tend to come later than the tracker says; < 0 means earlier.

# COMMAND ----------

# DBTITLE 1,Error histogram: when it's wrong, by how much?
display(spark.sql(f"""
SELECT error_min, prediction_outcome, sum(predictions) AS predictions
FROM {TBL_TRAIN_GOLD_ERROR_DIST}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
ORDER BY error_min
"""))
# Tip: switch this output to a bar chart (+ → Visualization), x = error_min, y = predictions, group by prediction_outcome.

# COMMAND ----------

# DBTITLE 1,Bus vs train: on-time rate by time of day (weekdays)
display(spark.sql(f"""
SELECT time_period, mode,
       sum(predictions)                                             AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)  AS on_time_pct,
       round(sum(sum_abs_error_min) / sum(predictions), 2)          AS avg_minutes_off
FROM (
  SELECT 'Bus' AS mode, * FROM {TBL_GOLD_ACCURACY}
  UNION ALL
  SELECT 'Train' AS mode, * FROM {TBL_TRAIN_GOLD_ACCURACY}
)
WHERE {DATE_FILTER} AND {BUCKET_FILTER} AND day_type = 'Weekday'
GROUP BY ALL
ORDER BY time_period, mode
"""))