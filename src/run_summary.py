# Databricks notebook source
# Campaign Runner: run_summary
# Reports this run's campaigns and egress result, and fails the job if any campaign failed, so a bad rule or
# template shows up as a red run instead of a silent gap. run_history is a view over run_log, so it is current
# as soon as each campaign logs; nothing needs refreshing.

# COMMAND ----------

# MAGIC %run ./pco_lib

# COMMAND ----------

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("run_id", "manual")
run_date = parse_run_date(dbutils.widgets.get("run_date"))
run_id = dbutils.widgets.get("run_id").strip()

rows = spark.sql(f"""
    SELECT status, count(*) AS campaigns, sum(messages_final) AS messages, sum(rows_inserted) AS inserted,
           sum(rows_updated) AS updated, sum(rows_retracted) AS retracted
    FROM {T('run_log')} WHERE run_date = DATE'{run_date.isoformat()}' AND run_id = '{run_id}' GROUP BY status
""").collect()
by_status = {r["status"]: r.asDict() for r in rows}
failed = int(by_status.get("FAILED", {}).get("campaigns", 0) or 0)
summary = {"run_date": run_date.isoformat(), "run_id": run_id, "by_status": by_status}
try:
    summary["egress"] = json.loads(dbutils.jobs.taskValues.get(taskKey="egress", key="egress_result", default="{}"))
    summary["maintenance"] = json.loads(dbutils.jobs.taskValues.get(taskKey="maintenance", key="maintenance_result",
                                                                    default="{}"))
except Exception:  # noqa: BLE001  (task values are only available inside a job run)
    pass
print(json.dumps(summary, indent=2, default=str))
display(spark.sql(f"""
    SELECT campaign_id, campaign_name, status, audience_count, after_lookbacks, after_targeting, after_exclusions,
           messages_final, rows_inserted, rows_updated, rows_retracted, error_message
    FROM {T('run_log')} WHERE run_date = DATE'{run_date.isoformat()}' AND run_id = '{run_id}'
    ORDER BY status, campaign_id
"""))
if failed:
    raise Exception(f"{failed} campaign(s) failed for {run_date}; see run_history for run {run_id}")
