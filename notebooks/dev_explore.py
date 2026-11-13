# Databricks notebook source
# MAGIC %md
# MAGIC # Dev · Explore and check the pipeline
# MAGIC
# MAGIC Read-only scratch notebook: nothing here writes to any table, so run any cell, any time.
# MAGIC Not part of a job.
# MAGIC
# MAGIC | Section | Use it to |
# MAGIC |---|---|
# MAGIC | 1. Pipeline health | See if the jobs are running, how fresh the data is, and spot polling gaps |
# MAGIC | 2. Table overview | Row counts and date ranges for every layer |
# MAGIC | 3. Raw data | Look at the latest poll exactly as CTA sent it |
# MAGIC | 4. Arrival detection | See how bus visits ended, and trace one visit poll by poll |
# MAGIC | 5. Results | The project question: accuracy by time of day, stop and route |
# MAGIC
# MAGIC Use the widgets at the top to narrow the date range or focus on one stop or route.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# DBTITLE 1,Filters
dbutils.widgets.text("days_back", "7", "Days back")
dbutils.widgets.text("stop_id", "", "Stop ID (blank = all)")
dbutils.widgets.text("route_id", "", "Route (blank = all)")
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

print(f"Last {DAYS_BACK} days | stop: {STOP_ID or 'all'} | route: {ROUTE_ID or 'all'} | tracker said: {WAIT_BUCKET}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline health

# COMMAND ----------

# DBTITLE 1,Freshness: how old is the newest data in each layer?
display(spark.sql(f"""
SELECT 'bronze.bus_predictions_raw' AS table_name, max(polled_at) AS newest,
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(polled_at))) / 60, 1) AS minutes_ago
FROM {TBL_BRONZE_PREDICTIONS}
UNION ALL
SELECT 'silver.bus_predictions', max(polled_at),
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(polled_at))) / 60, 1)
FROM {TBL_PREDICTIONS}
UNION ALL
SELECT 'silver.bus_arrivals', max(last_seen_at),
       round((unix_timestamp(current_timestamp()) - unix_timestamp(max(last_seen_at))) / 60, 1)
FROM {TBL_ARRIVALS}
"""))
# Healthy: bronze < 5 min old, silver < ~70 min (hourly job), arrivals a bit older than silver.

# COMMAND ----------

# DBTITLE 1,Ingestion runs per hour (want ~30/hour at 2-minute polling)
display(spark.sql(f"""
SELECT date_trunc('hour', started_at)        AS hour,
       count(*)                              AS runs,
       count_if(status = 'partial')          AS partial_runs,
       count_if(status = 'failed')           AS failed_runs,
       sum(api_calls)                        AS api_calls,
       sum(predictions_landed)               AS predictions_landed
FROM {TBL_INGESTION_RUNS}
WHERE started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY hour DESC
"""))

# COMMAND ----------

# DBTITLE 1,API calls per day vs the 10,000/day key limit
display(spark.sql(f"""
SELECT to_date(started_at) AS day,
       sum(api_calls)      AS api_calls,
       round(100 * sum(api_calls) / 10000, 1) AS pct_of_daily_limit
FROM {TBL_INGESTION_RUNS}
WHERE started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY day DESC
"""))

# COMMAND ----------

# DBTITLE 1,Recent problems: runs that weren't clean
display(spark.sql(f"""
SELECT started_at, status, api_calls, failed_api_calls, predictions_landed, api_messages
FROM {TBL_INGESTION_RUNS}
WHERE status <> 'success' AND started_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
ORDER BY started_at DESC
LIMIT 50
"""))
# 'No service scheduled' messages late at night are normal; they only mean a stop has no buses then.

# COMMAND ----------

# DBTITLE 1,Polling gaps: minutes between consecutive polls (anything over 3 breaks arrival detection)
display(spark.sql(f"""
WITH polls AS (
  SELECT DISTINCT polled_at FROM {TBL_PREDICTIONS} WHERE {DATE_FILTER}
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
    ("bronze", TBL_BRONZE_PREDICTIONS, "to_date(polled_at)"),
    ("silver", TBL_PREDICTIONS, "local_date"),
    ("silver", TBL_ARRIVALS, "local_date"),
    ("silver", TBL_OUTCOMES, "local_date"),
    ("gold", TBL_GOLD_ACCURACY, "local_date"),
    ("gold", TBL_GOLD_ERROR_DIST, "local_date"),
    ("gold", TBL_GOLD_ARRIVAL_QUALITY, "local_date"),
]
display(spark.sql(" UNION ALL ".join(
    f"SELECT '{layer}' AS layer, '{t}' AS table_name, count(*) AS row_count, "
    f"min({d}) AS first_date, max({d}) AS last_date FROM {t}"
    for layer, t, d in tables
)))

# COMMAND ----------

# DBTITLE 1,Tracked stops (from discovery)
display(spark.sql(f"""
SELECT stop_id, stop_label, area, is_active, discovered_via_routes, last_discovered_at
FROM {TBL_STOPS}
ORDER BY is_active DESC, area, stop_label
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Raw data

# COMMAND ----------

# DBTITLE 1,Latest poll, exactly as CTA returned it
display(spark.sql(f"""
SELECT polled_at, stpid, stpnm, rt, rtdir, vid, tatripid, prdtm, prdctdn, dstp, dly, typ
FROM {TBL_BRONZE_PREDICTIONS}
WHERE polled_at = (SELECT max(polled_at) FROM {TBL_BRONZE_PREDICTIONS})
  AND {STOP_FILTER.replace('stop_id', 'stpid')}
  AND {ROUTE_FILTER.replace('route_id', 'rt')}
ORDER BY stpnm, try_cast(prdctdn AS INT)
"""))

# COMMAND ----------

# DBTITLE 1,Fields CTA sent that aren't in the schema (should be empty or rare)
display(spark.sql(f"""
SELECT _rescued_data, count(*) AS rows
FROM {TBL_BRONZE_PREDICTIONS}
WHERE _rescued_data IS NOT NULL AND polled_at >= current_timestamp() - INTERVAL {DAYS_BACK} DAYS
GROUP BY ALL
ORDER BY rows DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Arrival detection

# COMMAND ----------

# DBTITLE 1,How did bus visits end, per day?
display(spark.sql(f"""
SELECT local_date,
       count(*)                                                       AS visits,
       round(100 * count_if(arrival_status = 'arrived')  / count(*), 1) AS arrived_pct,
       round(100 * count_if(arrival_status = 'vanished') / count(*), 1) AS vanished_pct,
       round(100 * count_if(arrival_status = 'censored') / count(*), 1) AS censored_pct
FROM {TBL_ARRIVALS}
WHERE {WHERE}
GROUP BY ALL
ORDER BY local_date DESC
"""))
# Healthy with continuous polling: mostly arrived. High censored = polling gaps. High vanished = check last_distance_ft below.

# COMMAND ----------

# DBTITLE 1,Last distance before the bus dropped out (is the 1,500 ft cutoff right?)
display(spark.sql(f"""
SELECT least(floor(last_distance_ft / 250) * 250, 5000) AS last_distance_ft_bucket,
       arrival_status,
       count(*) AS visits
FROM {TBL_ARRIVALS}
WHERE {WHERE} AND arrival_status <> 'censored'
GROUP BY ALL
ORDER BY last_distance_ft_bucket
"""))
# Most visits should drop out close to the stop. A big bump just above 1,500 ft would mean the cutoff is too strict.

# COMMAND ----------

# DBTITLE 1,Trace one arrived visit poll by poll
one = spark.sql(f"""
SELECT arrival_id FROM {TBL_ARRIVALS}
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
           o.prediction_outcome, a.route_id, a.stop_name, a.vehicle_id
    FROM {TBL_OUTCOMES} o JOIN {TBL_ARRIVALS} a USING (arrival_id)
    WHERE arrival_id = '{one.arrival_id}'
    ORDER BY o.polled_at
    """))
# Each row is one moment a rider could have checked the app. Error should shrink as the bus gets closer.

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
FROM {TBL_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
ORDER BY day_type, time_period
"""))

# COMMAND ----------

# DBTITLE 1,Heatmap: on-time % by hour x how far out the prediction was
heat = spark.sql(f"""
SELECT hour_label, predicted_wait_bucket,
       round(100 * sum(on_time_predictions) / sum(predictions), 0) AS on_time_pct
FROM {TBL_GOLD_ACCURACY}
WHERE {WHERE}
GROUP BY ALL
""")
display(heat.groupBy("hour_label").pivot("predicted_wait_bucket").agg(F.first("on_time_pct")).orderBy("hour_label"))
# Read across a column: accuracy through the day. Read across a row: how accuracy fades the further out the prediction is.

# COMMAND ----------

# DBTITLE 1,Stops ranked by reliability (min 50 predictions)
display(spark.sql(f"""
SELECT stop_label, area,
       sum(predictions)                                             AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)  AS on_time_pct,
       round(100 * sum(early_predictions)   / sum(predictions), 1)  AS early_pct,
       round(sum(sum_abs_error_min) / sum(predictions), 2)          AS avg_minutes_off
FROM {TBL_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
HAVING sum(predictions) >= 50
ORDER BY on_time_pct DESC
"""))

# COMMAND ----------

# DBTITLE 1,Routes ranked by reliability (min 50 predictions)
display(spark.sql(f"""
SELECT route_label,
       sum(predictions)                                             AS predictions,
       round(100 * sum(on_time_predictions) / sum(predictions), 1)  AS on_time_pct,
       round(sum(sum_error_min) / sum(predictions), 2)              AS avg_bias_min,
       round(sum(sum_abs_error_min) / sum(predictions), 2)          AS avg_minutes_off
FROM {TBL_GOLD_ACCURACY}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
HAVING sum(predictions) >= 50
ORDER BY on_time_pct DESC
"""))
# avg_bias_min > 0 means buses on that route tend to come later than the tracker says; < 0 means earlier.

# COMMAND ----------

# DBTITLE 1,Error histogram: when it's wrong, by how much?
display(spark.sql(f"""
SELECT error_min, prediction_outcome, sum(predictions) AS predictions
FROM {TBL_GOLD_ERROR_DIST}
WHERE {WHERE} AND {BUCKET_FILTER}
GROUP BY ALL
ORDER BY error_min
"""))
# Tip: switch this output to a bar chart (+ → Visualization), x = error_min, y = predictions, group by prediction_outcome.