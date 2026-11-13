# Databricks notebook source
# MAGIC %md
# MAGIC # publish_site
# MAGIC Last task of the hourly transform job. Builds small summary numbers from the gold rider tables and
# MAGIC pushes them to `docs/data.json` in your public GitHub repo. GitHub Pages serves the web page that reads it.
# MAGIC
# MAGIC Needs a GitHub token in the secret scope: scope `cta`, key `github_token`.

# COMMAND ----------

dbutils.widgets.text("repo", "yilunr/cta-arrival-accuracy", "GitHub repo (owner/name)")
dbutils.widgets.text("branch", "main", "Branch")
dbutils.widgets.text("window_days", "14", "Days of history shown")
REPO, BRANCH, DAYS = dbutils.widgets.get("repo"), dbutils.widgets.get("branch"), int(dbutils.widgets.get("window_days"))

# COMMAND ----------

import json, base64, datetime, requests
from pyspark.sql import functions as F

def rates(df, *keys):
    """One row per key combination: prediction count and the plain-average rates."""
    return (df.groupBy(*keys).agg(
        F.count("*").alias("n"),
        F.round(F.avg("on_time") * 100, 1).alias("on_time"),
        F.round(F.avg("is_early") * 100, 1).alias("early"),
        F.round(F.avg("is_late") * 100, 1).alias("late"),
        F.round(F.avg("abs_error_min"), 2).alias("abs_err")))

def rows(df):
    return [r.asDict() for r in df.collect()]

cutoff = F.date_sub(F.current_date(), DAYS)
train = spark.table("cta_data.gold.train_rider_predictions").where(F.col("local_date") >= cutoff)
bus   = spark.table("cta_data.gold.rider_predictions").where(F.col("local_date") >= cutoff)

# Hide platforms/routes with too little data, so a handful of predictions can't show as "100%".
MIN_N = 50

def summary(df, entity_cols):
    overall = rows(rates(df.withColumn("k", F.lit(1)), "k"))[0]
    overall.pop("k")
    return overall

train_platforms = rates(train, "stop_id", "stop_label", "area").where(f"n >= {MIN_N}")
train_hours = rates(train, "stop_id", "hour_of_day").where("n >= 5")

data = {
    "updated_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    "window_days": DAYS,
    "train": {
        "overall":   summary(train, []),
        "by_hour":   rows(rates(train, "hour_of_day").orderBy("hour_of_day")),
        "by_line":   rows(rates(train, "route_label").orderBy(F.desc("n"))),
        "platforms": rows(train_platforms.orderBy("stop_label")),
        "platform_hours": rows(train_hours.orderBy("stop_id", "hour_of_day")),
    },
    "bus": {
        "overall":  summary(bus, []),
        "by_hour":  rows(rates(bus, "hour_of_day").orderBy("hour_of_day")),
        "by_route": rows(rates(bus, "route_label").where(f"n >= {MIN_N}").orderBy(F.desc("n")).limit(15)),
    },
}
payload = json.dumps(data, separators=(",", ":"), default=str)
print(f"data.json is {len(payload)/1024:.0f} KB; train platforms: {len(data['train']['platforms'])}")

# COMMAND ----------

# Push to GitHub (create or update docs/data.json).
token = dbutils.secrets.get("cta", "github_token")
url = f"https://api.github.com/repos/{REPO}/contents/docs/data.json"
hdr = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
cur = requests.get(url, headers=hdr, params={"ref": BRANCH}, timeout=30)
body = {"message": f"Update data {data['updated_utc']} UTC", "branch": BRANCH,
        "content": base64.b64encode(payload.encode()).decode()}
if cur.status_code == 200:
    body["sha"] = cur.json()["sha"]
elif cur.status_code != 404:
    raise RuntimeError(f"GitHub read failed: HTTP {cur.status_code}")
put = requests.put(url, headers=hdr, json=body, timeout=60)
if put.status_code not in (200, 201):
    raise RuntimeError(f"GitHub push failed: HTTP {put.status_code} {put.text[:200]}")
print("Published docs/data.json")
