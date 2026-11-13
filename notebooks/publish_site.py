# Databricks notebook source
# MAGIC %md
# MAGIC # publish_site
# MAGIC Last task of the hourly train transform job. Exports a compact summary of the gold tables to
# MAGIC `docs/data.json` in the public GitHub repo. The GitHub Pages site reads that file and applies
# MAGIC its filters (days, time of day, area, platform) in the browser, so viewers never touch Databricks.
# MAGIC
# MAGIC Needs a GitHub token in the secret scope: scope `cta`, key `github_token` (Contents: read and write).

# COMMAND ----------

dbutils.widgets.text("repo", "yilunr/cta-arrival-accuracy", "GitHub repo (owner/name)")
dbutils.widgets.text("branch", "main", "Branch")
dbutils.widgets.text("window_days", "14", "Days of history shown")
REPO, BRANCH, DAYS = dbutils.widgets.get("repo"), dbutils.widgets.get("branch"), int(dbutils.widgets.get("window_days"))

# COMMAND ----------

import json, base64, requests
from pyspark.sql import functions as F

cutoff = F.date_sub(F.current_date(), DAYS)
# Monday = 1 ... Sunday = 7
DOW = ((F.dayofweek("local_date") + 5) % 7) + 1

def export_mode(rider_table, label_df=None):
    """Stops list + a (stop, day of week, hour) cube of counts + a (stop, route) table."""
    df = spark.table(rider_table).where(F.col("local_date") >= cutoff)
    if label_df is not None:  # nicer stop labels (bus: name, direction, stop number)
        df = df.join(label_df, "stop_id", "left").withColumn("stop_label", F.coalesce("nice_label", "stop_label")).drop("nice_label")
    df = df.withColumn("dow", DOW)
    stops = (df.groupBy("stop_id").agg(F.max("stop_label").alias("label"), F.max("area").alias("area"),
                                        F.round(F.avg("latitude"), 5).alias("lat"), F.round(F.avg("longitude"), 5).alias("lon"))
               .orderBy("label").collect())
    idx = {r["stop_id"]: i for i, r in enumerate(stops)}
    counts = [F.count("*").alias("n"), F.sum("is_early").cast("int").alias("e"), F.sum("is_late").cast("int").alias("l")]
    cube = df.groupBy("stop_id", "dow", "hour_of_day").agg(*counts, F.round(F.sum("abs_error_min") * 10).cast("int").alias("a")).collect()
    routes = df.groupBy("stop_id", "route_label").agg(*counts).collect()
    # Histogram of minutes off (0.25-minute bins, last bin = 4+ min) so the page can show a true median for any filter.
    period = F.expr("CASE WHEN hour_of_day < 5 THEN 0 WHEN hour_of_day < 7 THEN 1 WHEN hour_of_day < 10 THEN 2 "
                    "WHEN hour_of_day < 15 THEN 3 WHEN hour_of_day < 19 THEN 4 ELSE 5 END")
    hist = {}
    for r in (df.where(F.col("abs_error_min").isNotNull())
                .groupBy("stop_id", "dow", period.alias("p"), F.least(F.floor(F.col("abs_error_min") / 0.25), F.lit(16)).cast("int").alias("b"))
                .count().collect()):
        hist.setdefault((idx[r["stop_id"]], r["dow"], r["p"]), [0] * 17)[r["b"]] = r["count"]
    return {
        "stops":  [{"label": r["label"], "area": r["area"], "lat": r["lat"], "lon": r["lon"]} for r in stops],
        # [stop index, day of week, hour, predictions, early, late, sum of minutes off x10]
        "cube":   [[idx[r["stop_id"]], r["dow"], r["hour_of_day"], r["n"], r["e"], r["l"], r["a"]] for r in cube],
        # [stop index, route, predictions, early, late]
        "routes": [[idx[r["stop_id"]], r["route_label"], r["n"], r["e"], r["l"]] for r in routes],
        # [stop index, day of week, time-of-day period 0-5, then 17 bin counts]
        "hist":   [list(k) + v for k, v in hist.items()],
    }

def export_quality(quality_table, visits_col, runs_table):
    q = (spark.table(quality_table).where(F.col("local_date") >= cutoff).groupBy("local_date")
           .agg(F.sum(visits_col).alias("v"), F.sum("arrived_visits").alias("a"),
                F.sum("vanished_visits").alias("x"), F.sum("censored_visits").alias("c")).orderBy("local_date").collect())
    runs = spark.table(runs_table)
    local = F.from_utc_timestamp("started_at", "America/Chicago")
    per_day = (runs.where(F.col("started_at") >= F.current_timestamp() - F.expr("INTERVAL 7 DAYS"))
                 .groupBy(F.date_format(local, "yyyy-MM-dd").alias("d"))
                 .agg(F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("ok"),
                      F.sum(F.when(F.col("status") == "partial", 1).otherwise(0)).alias("partial"),
                      F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed")).orderBy("d").collect())
    mins = runs.agg(F.floor((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("started_at"))) / 60)).first()[0]
    return {"days": [[str(r["local_date"]), int(r["v"] or 0), int(r["a"] or 0), int(r["x"] or 0), int(r["c"] or 0)] for r in q],
            "runs": [[r["d"], r["ok"], r["partial"], r["failed"]] for r in per_day],
            "minutes_since_poll": int(mins) if mins is not None else None}

# Bus stop labels with direction of travel, e.g. "Adams & Wabash · SB (#77)" (same as the dashboard)
bus_labels = spark.sql("""
  SELECT stop_id, concat(stop_name, ' · ',
         CASE direction WHEN 'Northbound' THEN 'NB' WHEN 'Southbound' THEN 'SB'
                        WHEN 'Eastbound' THEN 'EB' WHEN 'Westbound' THEN 'WB' ELSE direction END,
         ' (#', stop_id, ')') AS nice_label
  FROM cta_data.silver.stops""")

data = {
    "version": 2,
    "updated": spark.sql("SELECT date_format(from_utc_timestamp(current_timestamp(), 'America/Chicago'), 'MMM d, h:mm a')").first()[0],
    "window_days": DAYS,
    "train": export_mode("cta_data.gold.train_rider_predictions"),
    "bus":   export_mode("cta_data.gold.rider_predictions", bus_labels),
    "quality": {
        "train": export_quality("cta_data.gold.train_arrival_quality", "train_visits", "cta_data.ops.train_ingestion_runs"),
        "bus":   export_quality("cta_data.gold.arrival_quality", "bus_visits", "cta_data.ops.ingestion_runs"),
    },
}
payload = json.dumps(data, separators=(",", ":"), default=str)
print(f"data.json: {len(payload)/1024:.0f} KB | train platforms {len(data['train']['stops'])}, bus stops {len(data['bus']['stops'])}")

# COMMAND ----------

# Push to GitHub (create or update docs/data.json).
token = dbutils.secrets.get("cta", "github_token")
url = f"https://api.github.com/repos/{REPO}/contents/docs/data.json"
hdr = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
cur = requests.get(url, headers=hdr, params={"ref": BRANCH}, timeout=30)
body = {"message": f"Update data ({data['updated']} Chicago)", "branch": BRANCH,
        "content": base64.b64encode(payload.encode()).decode()}
if cur.status_code == 200:
    body["sha"] = cur.json()["sha"]
elif cur.status_code != 404:
    raise RuntimeError(f"GitHub read failed: HTTP {cur.status_code}")
put = requests.put(url, headers=hdr, json=body, timeout=60)
if put.status_code not in (200, 201):
    raise RuntimeError(f"GitHub push failed: HTTP {put.status_code} {put.text[:200]}")
print("Published docs/data.json")
