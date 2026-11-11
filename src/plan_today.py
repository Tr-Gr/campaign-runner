# Databricks notebook source
# Campaign Runner: plan_today
# 1. Picks the campaigns due on run_date (Quartz cron, start and end dates, active flag, approval gate).
# 2. Rebuilds the 30-day calendar so marketing sees what is scheduled.
# 3. Publishes the due campaign ids as a task value for the for-each fan-out.

# COMMAND ----------

# MAGIC %run ./pco_lib

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = parse_run_date(dbutils.widgets.get("run_date"))

calendar_rows = build_calendar(run_date, days=30)
due, waiting = [], []
for c in active_campaigns():
    if campaign_send_times(c, run_date):
        (due if is_approved(c) else waiting).append(c["campaign_id"])
due = sorted(due)

dbutils.jobs.taskValues.set(key="due_campaigns", value=due)
dbutils.jobs.taskValues.set(key="run_date", value=run_date.isoformat())
print(f"run_date={run_date} due={len(due)} awaiting_approval={sorted(waiting)} calendar_rows={calendar_rows}")
print(due)
