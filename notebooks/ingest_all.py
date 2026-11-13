# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingest · Bus and train in one run (job: every 2 minutes)
# MAGIC
# MAGIC Runs `bronze` (bus) and `train_bronze` (train) from a single job run, side by side.
# MAGIC One job run means one serverless startup per cycle instead of two, which is where most of the
# MAGIC daily usage goes on Free Edition. Neither notebook is changed; each still writes its own
# MAGIC tables and its own row in the ops run log.
# MAGIC
# MAGIC * If one mode fails, the other still runs. The job run is marked failed afterwards so you see it.
# MAGIC * This notebook must sit in the same folder as `bronze` and `train_bronze`.
# MAGIC
# MAGIC **Job settings:** every 2 min, max concurrent runs = 1, 1 retry. Pause the two separate ingest
# MAGIC jobs once this one is running, or everything gets polled twice.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("timeout_sec", "600", "Timeout per notebook (seconds)")
TIMEOUT_SEC = int(dbutils.widgets.get("timeout_sec"))

NOTEBOOKS = {"bus": "./bronze", "train": "./train_bronze"}

# COMMAND ----------

# DBTITLE 1,Run both ingest notebooks at the same time
from concurrent.futures import ThreadPoolExecutor


def run_ingest(item):
    mode, path = item
    try:
        dbutils.notebook.run(path, TIMEOUT_SEC)
        return mode, None
    except Exception as e:
        return mode, f"{type(e).__name__}: {str(e)[:500]}"


with ThreadPoolExecutor(max_workers=len(NOTEBOOKS)) as pool:
    results = dict(pool.map(run_ingest, NOTEBOOKS.items()))

for mode, error in results.items():
    print(f"{mode}: {'ok' if error is None else 'FAILED - ' + error}")

# COMMAND ----------

# DBTITLE 1,Fail the run if either mode failed
failed = {mode: error for mode, error in results.items() if error}
if failed:
    raise RuntimeError(f"Ingest failed for: {', '.join(failed)}. Details are in the output above and in the ops run logs.")