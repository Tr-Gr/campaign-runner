# Databricks notebook source
# Campaign Runner: run_campaign (one campaign, one run date)
# Runs the shared pipeline from pco_lib in the same order as the existing shared modules, then MERGEs the result
# into message_approved. Failures are logged as FAILED and do not stop other campaigns; run_summary fails the job.

# COMMAND ----------

# MAGIC %run ./pco_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("campaign_id", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("run_id", "manual")
campaign_id = dbutils.widgets.get("campaign_id").strip()
run_date = parse_run_date(dbutils.widgets.get("run_date"))
run_id = dbutils.widgets.get("run_id").strip() or "manual"

started_at, cfg, tpl = datetime.now(), None, None
counts = dict(audience_count=0, after_lookbacks=0, after_targeting=0, after_exclusions=0, messages_final=0,
              rows_inserted=0, rows_updated=0, rows_retracted=0)
try:
    cfg = spark.table(T("campaign_config")).where(F.col("campaign_id") == campaign_id).collect()[0].asDict()
    tpl = (spark.table(T("templates")).where(f"template_id = '{cfg['template_id']}' AND active")
           .orderBy(F.col("template_version").desc()).limit(1).collect()[0].asDict())
    allowed = audience_columns()

    df = audience(cfg, allowed)                              # 1. audience query
    counts["audience_count"] = df.count()
    df = lookbacks(df, cfg, run_date)                        # 2a. lookbacks
    counts["after_lookbacks"] = df.count()
    df = target_to_recipient(df, cfg)                        # 2b. target to recipient, dedupe
    counts["after_targeting"] = df.count()
    df = standard_exclusions(df)                             # 2c. standard exclusions
    counts["after_exclusions"] = df.count()
    df = build_payload(df, cfg, tpl)                         # 2d. JSON payload
    df = segmentation(df, cfg, allowed)                      # 2e. segmentation (optional)
    df = schedule_messages(df, cfg, run_date)                # 2f. send window and expiration
    df = finalize(df, cfg, run_date, run_id)
    counts["messages_final"] = df.count()
    m = write_message_approved(df, cfg, run_date)            # 3. message_approved (MERGE)
    counts.update(rows_inserted=m["inserted"], rows_updated=m["updated"], rows_retracted=m["retracted"])
    status, err = "SUCCEEDED", None
except Exception as e:  # noqa: BLE001
    status, err = "FAILED", f"{type(e).__name__}: {str(e)[:500]}"
    print("FAILED", campaign_id, err)

finished_at = datetime.now()
log = [(str(uuid.uuid4()), run_id, run_date, campaign_id, cfg["campaign_name"] if cfg else None, status,
        *[int(counts[k]) for k in ["audience_count", "after_lookbacks", "after_targeting", "after_exclusions",
                                   "messages_final", "rows_inserted", "rows_updated", "rows_retracted"]],
        tpl["template_id"] if tpl else None, int(tpl["template_version"]) if tpl else None,
        started_at, finished_at, err)]
log_df = spark.createDataFrame(log, spark.table(T("run_log")).schema)
with_retry(lambda: log_df.write.mode("append").saveAsTable(T("run_log")), label="run_log")

result = {"campaign_id": campaign_id, "status": status, **counts,
          "seconds": round((finished_at - started_at).total_seconds(), 1), "error": err}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
