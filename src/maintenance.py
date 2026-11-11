# Databricks notebook source
# Campaign Runner: maintenance
# Replaces an age-based "delete rows older than N days" job with a delivery-aware one:
#   - deletes message_approved rows only if they were EGRESSED and egressed more than retention_days ago
#   - never deletes PENDING rows; rows not delivered by the end of their send window surface as NOT_DELIVERED
#     in run_exceptions so someone acts on them
#   - trims send_history (used by lookbacks) after history_retention_days

# COMMAND ----------

# MAGIC %run ./pco_lib

# COMMAND ----------

dbutils.widgets.text("retention_days", "7")
dbutils.widgets.text("history_retention_days", "400")
retention_days = int(dbutils.widgets.get("retention_days") or 7)
history_days = int(dbutils.widgets.get("history_retention_days") or 400)

deleted = spark.sql(f"""
    DELETE FROM {T('message_approved')}
    WHERE egress_status = 'EGRESSED' AND egressed_at < current_timestamp() - INTERVAL {retention_days} DAYS
""").collect()[0]["num_affected_rows"]
trimmed = spark.sql(f"""
    DELETE FROM {T('send_history')} WHERE sent_date < date_sub(current_date(), {history_days})
""").collect()[0]["num_affected_rows"]
undelivered = spark.sql(f"""
    SELECT count(*) AS n FROM {T('message_approved')}
    WHERE egress_status = 'PENDING' AND send_window_end < current_timestamp()
""").first()["n"]
pending = spark.sql(f"SELECT count(*) AS n FROM {T('message_approved')} WHERE egress_status = 'PENDING'").first()["n"]

result = {"deleted_egressed": int(deleted), "history_trimmed": int(trimmed), "pending_rows_kept": int(pending),
          "undelivered_past_window": int(undelivered), "retention_days": retention_days}
print(json.dumps(result))
dbutils.jobs.taskValues.set(key="maintenance_result", value=json.dumps(result))
