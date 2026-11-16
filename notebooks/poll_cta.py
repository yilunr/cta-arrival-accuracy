# Databricks notebook source
# MAGIC %md
# MAGIC # poll_cta — lightweight poller (runs every few minutes)
# MAGIC
# MAGIC Calls the CTA Bus Tracker and Train Tracker APIs and lands the raw responses as JSON-lines files in the
# MAGIC `bronze.landing` volume — the same files, in the same place and format, that `bronze` and `train_bronze` land.
# MAGIC Nothing else: no Auto Loader, no Delta writes, no table scans. Those moved to `load_landing`, which runs hourly
# MAGIC as the first task of each transform job. That is what keeps the every-few-minutes cost low enough for the
# MAGIC Free Edition daily usage cap.
# MAGIC
# MAGIC * The stop and station lists come from small cache files that `load_landing` refreshes every hour
# MAGIC   (`active_bus_stops.json`, `active_train_stations.json`). The first ever run reads the tables instead.
# MAGIC * Each run writes one small run-log JSON file per mode; `load_landing` loads those into `ops.ingestion_runs`
# MAGIC   and `ops.train_ingestion_runs`, so the Data quality page keeps working.
# MAGIC * Buses and trains are polled at the same time in two threads.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("polls_per_run", "1", "Polls per run")
dbutils.widgets.text("poll_interval_sec", "90", "Seconds between polls (when polls_per_run > 1)")
dbutils.widgets.dropdown("modes", "both", ["both", "bus", "train"], "What to poll")

POLLS_PER_RUN = int(dbutils.widgets.get("polls_per_run"))
POLL_INTERVAL_SEC = int(dbutils.widgets.get("poll_interval_sec"))
MODES = ["bus", "train"] if dbutils.widgets.get("modes") == "both" else [dbutils.widgets.get("modes")]

# COMMAND ----------

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

LANDING_ROOT = f"/Volumes/{CATALOG}/bronze/landing"
RUN_LOG_ROOT = f"{LANDING_ROOT}/run_logs"
STOP_CACHE = {"bus": f"{LANDING_ROOT}/active_bus_stops.json", "train": f"{LANDING_ROOT}/active_train_stations.json"}


def write_json_atomic(path, obj, lines=False):
    """Write a file so a reader (Auto Loader) never sees it half-written."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{os.path.dirname(path)}/.{os.path.basename(path)}.tmp"
    with open(tmp, "w") as f:
        if lines:
            f.write("\n".join(json.dumps(r) for r in obj) + "\n")
        else:
            json.dump(obj, f)
    os.rename(tmp, path)


def active_ids(mode):
    """Stop IDs (bus) or station IDs (train) to poll, from the hourly cache file; first run falls back to the table."""
    path = STOP_CACHE[mode]
    try:
        with open(path) as f:
            ids = json.load(f)
        if ids:
            return ids
    except FileNotFoundError:
        pass
    print(f"No cache at {path}; reading the table once (load_landing will write the cache)")
    if mode == "bus":
        ids = [r.stop_id for r in spark.table(TBL_STOPS).filter("is_active").select("stop_id").collect()]
    else:
        ids = sorted(r.station_id for r in spark.table(TBL_TRAIN_STOPS).filter("is_active")
                     .select("station_id").distinct().collect())
    if not ids:
        raise RuntimeError(f"No active {mode} stops. Run {'bronze' if mode == 'bus' else 'train_bronze'} with rediscover = true.")
    write_json_atomic(path, ids)
    return ids

# COMMAND ----------

# DBTITLE 1,Bus: one poll across all stop batches
def poll_bus_once(stop_ids):
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
        for err in body.get("error", []):
            msg = err.get("msg", "unknown error")
            messages.add(msg)
            if "limit" in msg.lower():
                raise RuntimeError(f"CTA says: {msg}. Stopping so the daily key quota is not burned further.")
        for prd in body.get("prd", []):
            records.append({**prd, "_poll_id": poll_id, "_polled_at_ms": polled_at_ms})

    if records:
        write_json_atomic(f"{LANDING_PATH}/{polled_at:%Y/%m/%d}/poll_{polled_at:%H%M%S}_{poll_id[:8]}.json", records, lines=True)
    return records, len(batches), failed, messages

# COMMAND ----------

# DBTITLE 1,Train: one poll across all station batches
FATAL_ERROR_CODES = {"101": "invalid API key", "102": "daily API limit reached"}


def request_arrivals(station_ids):
    """ttarrivals for up to MAX_STATIONS_PER_CALL stations; a rejected group is retried one station at a time."""
    predictions, calls, failed, messages = [], 0, 0, set()
    pending = [list(station_ids)]
    while pending:
        group = pending.pop(0)
        calls += 1
        try:
            body = call_cta_train("ttarrivals.aspx", mapid=group)
        except RuntimeError as e:
            failed += 1
            messages.add(str(e))
            continue
        code = str(body.get("errCd") or "0")
        if code == "0":
            eta = body.get("eta") or []
            predictions.extend([eta] if isinstance(eta, dict) else eta)
            continue
        message = f"{code}: {body.get('errNm')}"
        if code in FATAL_ERROR_CODES:
            raise RuntimeError(f"CTA says {message}. Stopping so the key is not hammered further.")
        failed += 1
        messages.add(message)
        if len(group) > 1:
            pending.extend([s] for s in group)
    return predictions, calls, failed, messages


def poll_train_once(station_ids):
    poll_id = uuid.uuid4().hex
    polled_at = datetime.now(timezone.utc)
    polled_at_ms = int(polled_at.timestamp() * 1000)
    records, api_calls, failed, messages = [], 0, 0, set()

    for batch in chunked(station_ids, MAX_STATIONS_PER_CALL):
        predictions, calls, batch_failed, batch_messages = request_arrivals(batch)
        api_calls += calls
        failed += batch_failed
        messages |= batch_messages
        for eta in predictions:
            record = {k: (v if v is None or isinstance(v, str) else json.dumps(v)) for k, v in eta.items()}
            records.append({**record, "_poll_id": poll_id, "_polled_at_ms": polled_at_ms})

    if records:
        write_json_atomic(f"{TRAIN_LANDING_PATH}/{polled_at:%Y/%m/%d}/poll_{polled_at:%H%M%S}_{poll_id[:8]}.json", records, lines=True)
    return records, api_calls, failed, messages

# COMMAND ----------

# DBTITLE 1,Poll both modes in parallel and write a run-log file for each
def run_mode(mode):
    ids = active_ids(mode)
    poll = poll_bus_once if mode == "bus" else poll_train_once
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    landed, api_calls, failed_calls, api_messages = 0, 0, 0, set()

    for i in range(POLLS_PER_RUN):
        if i > 0:
            time.sleep(POLL_INTERVAL_SEC)
        records, calls, failed, messages = poll(ids)
        landed += len(records)
        api_calls += calls
        failed_calls += failed
        api_messages |= messages
        print(f"[{mode}] poll {i + 1}/{POLLS_PER_RUN}: {len(records)} predictions, {failed}/{calls} calls failed")

    finished_at = datetime.now(timezone.utc)
    status = "failed" if api_calls and failed_calls >= api_calls else ("partial" if failed_calls else "success")
    log = {
        "run_id": run_id,
        "started_at_ms": int(started_at.timestamp() * 1000),
        "finished_at_ms": int(finished_at.timestamp() * 1000),
        "status": status,
        "polled": len(ids),          # stops (bus) or stations (train)
        "polls": POLLS_PER_RUN,
        "api_calls": api_calls,
        "failed_api_calls": failed_calls,
        "predictions_landed": landed,
        "api_messages": " | ".join(sorted(api_messages))[:2000] or None,
    }
    write_json_atomic(f"{RUN_LOG_ROOT}/{mode}/{started_at:%Y/%m/%d}/run_{started_at:%H%M%S}_{run_id[:8]}.json", log)
    return mode, status


with ThreadPoolExecutor(max_workers=len(MODES)) as pool:
    results = dict(pool.map(run_mode, MODES))

print(results)
failed_modes = [m for m, s in results.items() if s == "failed"]
if failed_modes:
    raise RuntimeError(f"Every CTA API call failed for: {', '.join(failed_modes)}. See the run-log files / ops tables.")
