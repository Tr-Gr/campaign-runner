# Databricks notebook source
# Campaign Runner: 00_setup (setup job; safe to re-run)
# Creates the config, message, history and log tables, the marketing views, and (optionally) sample campaigns.
# message_approved keeps the name of the existing table and has Delta change data feed on, so egress reads
# exactly what changed instead of using a created_ts watermark.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "campaigns")
dbutils.widgets.text("table_prefix", "camp_")
dbutils.widgets.text("seed_sample_campaigns", "true")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
SEED = dbutils.widgets.get("seed_sample_campaigns").strip().lower() == "true"
assert CAT and SCH, "Set the catalog and schema parameters"
T = lambda n: f"{CAT}.{SCH}.{P}{n}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")

# COMMAND ----------

ddl = [
f"""CREATE TABLE IF NOT EXISTS {T('campaign_config')} (
  campaign_id STRING NOT NULL, campaign_name STRING,
  campaign_type STRING COMMENT 'one_time | recurring',
  schedule_cron STRING COMMENT 'Quartz cron (sec min hour dom month dow), Central time',
  start_date DATE, end_date DATE,
  channel STRING COMMENT 'email | text | mail',
  template_id STRING,
  audience_rule STRING COMMENT 'SQL predicate over audience_source (validated: columns, literals, operators only)',
  segmentation_rules STRING COMMENT 'Optional JSON list of {{"segment": name, "rule": predicate}}; first match wins',
  lookback_days INT COMMENT 'Do not resend within this many days (0 = no lookback)',
  lookback_scope STRING COMMENT 'campaign | channel | any',
  send_window_hours INT, expiration_days INT, max_per_run INT,
  priority_class STRING COMMENT 'regulatory | operational | marketing',
  requires_approval BOOLEAN COMMENT 'If true, the campaign only runs once approved_by is set',
  approved_by STRING, approved_at TIMESTAMP,
  owner_team STRING, active BOOLEAN, updated_at TIMESTAMP
) COMMENT 'One row per campaign. Adding a campaign is an insert; the Campaign Runner job picks up due rows.'""",
f"""CREATE TABLE IF NOT EXISTS {T('templates')} (
  template_id STRING NOT NULL, template_version INT NOT NULL, template_name STRING, channel STRING,
  payload_fields STRING COMMENT 'Comma-separated audience_source columns to include in the JSON payload',
  active BOOLEAN, change_note STRING, updated_at TIMESTAMP
) COMMENT 'Versioned templates. The runner uses the highest active version; a change is a new version row.'""",
f"""CREATE TABLE IF NOT EXISTS {T('message_approved')} (
  message_guid STRING NOT NULL COMMENT 'sha2(campaign_id|run_date|person_id): stable across reruns',
  campaign_id STRING, campaign_name STRING, run_date DATE, run_id STRING, person_id STRING, channel STRING,
  recipient_address STRING, segment STRING, template_id STRING, template_version INT,
  payload STRING COMMENT 'JSON personalization fields', content_hash STRING,
  send_window_start TIMESTAMP, send_window_end TIMESTAMP, expires_at TIMESTAMP,
  egress_status STRING COMMENT 'PENDING | EGRESSED', egress_batch_id STRING, egressed_at TIMESTAMP,
  created_ts TIMESTAMP, updated_ts TIMESTAMP
) CLUSTER BY (campaign_id, run_date)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true',
               'delta.enableRowTracking' = 'true')
COMMENT 'Approved messages ready for delivery. Egress reads the change data feed and marks rows EGRESSED.'""",
f"""CREATE TABLE IF NOT EXISTS {T('send_history')} (
  message_guid STRING, person_id STRING, campaign_id STRING, channel STRING, sent_date DATE,
  egress_batch_id STRING, recorded_at TIMESTAMP
) CLUSTER BY (sent_date) COMMENT 'What was delivered. Used by lookbacks; kept longer than message_approved.'""",
f"""CREATE TABLE IF NOT EXISTS {T('suppression_list')} (person_id STRING, reason STRING, added_at TIMESTAMP)
COMMENT 'Global suppression list applied by standard_exclusions'""",
f"""CREATE TABLE IF NOT EXISTS {T('run_log')} (
  run_log_id STRING, run_id STRING, run_date DATE, campaign_id STRING, campaign_name STRING, status STRING,
  audience_count BIGINT, after_lookbacks BIGINT, after_targeting BIGINT, after_exclusions BIGINT,
  messages_final BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_retracted BIGINT,
  template_id STRING, template_version INT, started_at TIMESTAMP, finished_at TIMESTAMP, error_message STRING
) COMMENT 'One row per campaign per run, with counts after each pipeline stage'""",
f"""CREATE TABLE IF NOT EXISTS {T('calendar')} (
  send_date DATE, send_time_ct STRING, campaign_id STRING, campaign_name STRING, channel STRING,
  priority_class STRING, owner_team STRING, max_pieces BIGINT, approved BOOLEAN, refreshed_at TIMESTAMP
) COMMENT 'Scheduled sends for the next 30 days, rebuilt by plan_today on every run'""",
f"""CREATE TABLE IF NOT EXISTS {T('egress_checkpoint')} (
  target_name STRING, last_version BIGINT, updated_at TIMESTAMP
) COMMENT 'Last message_approved table version delivered to each target'""",
f"""CREATE TABLE IF NOT EXISTS {T('egress_batches')} (
  batch_id STRING, run_id STRING, target_name STRING, method STRING, from_version BIGINT, to_version BIGINT,
  rows_sent BIGINT, rows_new BIGINT, rows_updated BIGINT, started_at TIMESTAMP, finished_at TIMESTAMP,
  status STRING, error_message STRING
) COMMENT 'One row per egress batch'""",
]
for s in ddl:
    spark.sql(s)

# COMMAND ----------

# Marketing views: campaign-level only, no person rows. The Genie Agent and dashboard read these.
spark.sql(f"""
CREATE OR REPLACE VIEW {T('run_history')} COMMENT 'One row per campaign per run (always current: a view over run_log)' AS
SELECT l.run_date, l.run_id, l.campaign_id, l.campaign_name, c.priority_class, c.owner_team, c.channel,
       l.status AS campaign_status, l.audience_count, l.messages_final, l.rows_inserted, l.rows_updated,
       l.rows_retracted, l.template_id, l.template_version, l.started_at, l.finished_at,
       round(timestampdiff(SECOND, l.started_at, l.finished_at) / 60.0, 1) AS minutes_to_run, l.error_message
FROM {T('run_log')} l LEFT JOIN {T('campaign_config')} c ON l.campaign_id = c.campaign_id
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {T('message_summary')} COMMENT 'Message counts by run date, campaign, channel, segment and delivery status' AS
SELECT m.run_date, m.campaign_id, m.campaign_name, c.priority_class, c.owner_team, m.channel, m.segment,
       m.template_id, m.template_version, m.egress_status, count(*) AS pieces
FROM {T('message_approved')} m LEFT JOIN {T('campaign_config')} c ON m.campaign_id = c.campaign_id
GROUP BY ALL
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {T('run_exceptions')} COMMENT 'Scheduled sends that failed, are late, pending, awaiting approval, or not delivered' AS
WITH runs AS (
  SELECT run_date, campaign_id, max(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS any_success,
         max(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS any_failed, max(error_message) AS error_message
  FROM {T('run_log')} GROUP BY run_date, campaign_id
), undelivered AS (
  SELECT run_date, campaign_id, count(*) AS pending_pieces FROM {T('message_approved')}
  WHERE egress_status = 'PENDING' AND send_window_end < current_timestamp() GROUP BY run_date, campaign_id
)
SELECT cal.send_date, cal.send_time_ct, cal.campaign_id, cal.campaign_name, cal.priority_class, cal.owner_team,
       cal.channel, cal.max_pieces,
       CASE WHEN NOT cal.approved THEN 'AWAITING_APPROVAL'
            WHEN r.any_failed = 1 AND coalesce(r.any_success, 0) = 0 THEN 'FAILED'
            WHEN u.pending_pieces > 0 THEN 'NOT_DELIVERED'
            WHEN r.any_success = 1 THEN 'OK'
            WHEN cal.send_date < current_date() THEN 'LATE'
            WHEN cal.send_date = current_date() THEN 'PENDING'
            ELSE 'UPCOMING' END AS exception_state,
       u.pending_pieces, r.error_message
FROM {T('calendar')} cal
LEFT JOIN runs r ON cal.campaign_id = r.campaign_id AND cal.send_date = r.run_date
LEFT JOIN undelivered u ON cal.campaign_id = u.campaign_id AND cal.send_date = u.run_date
""")

# COMMAND ----------

if SEED:
    templates = f"""SELECT * FROM VALUES
      ('TPL-BIRTHDAY-EMAIL', 1, 'Birthday greeting', 'email', 'first_name,clinic_name', true, 'initial'),
      ('TPL-BIRTHDAY-CARD', 1, 'Birthday card', 'mail', 'first_name,clinic_name', true, 'initial'),
      ('TPL-AWV-TEXT', 1, 'Annual wellness visit reminder', 'text', 'first_name,clinic_name,plan_type', true, 'initial'),
      ('TPL-WELCOME-EMAIL', 1, 'New member welcome', 'email', 'first_name,plan_type,clinic_name', true, 'initial'),
      ('TPL-CHECKUP-MAIL', 1, 'Overdue checkup letter', 'mail', 'first_name,clinic_name,last_encounter_date', true, 'initial'),
      ('TPL-FLU-TEXT', 1, 'Flu shot reminder', 'text', 'first_name,clinic_name', true, 'initial'),
      ('TPL-RENEWAL-MAIL', 1, 'Coverage renewal notice', 'mail', 'first_name,plan_type,group_id', true, 'initial'),
      ('TPL-AEP-MAIL', 1, 'Annual enrollment reminder', 'mail', 'first_name,plan_type', true, 'initial'),
      ('TPL-PREVENTIVE-EMAIL', 1, 'Preventive care reminder', 'email', 'first_name,clinic_name,preferred_language', true, 'initial'),
      ('TPL-PORTAL-EMAIL', 1, 'Patient portal sign-up', 'email', 'first_name', true, 'initial')
      AS t(template_id, template_version, template_name, channel, payload_fields, active, change_note)"""
    spark.sql(f"""MERGE INTO {T('templates')} t USING ({templates}) s
                  ON t.template_id = s.template_id AND t.template_version = s.template_version
                  WHEN NOT MATCHED THEN INSERT (template_id, template_version, template_name, channel, payload_fields,
                    active, change_note, updated_at)
                  VALUES (s.template_id, s.template_version, s.template_name, s.channel, s.payload_fields, s.active,
                    s.change_note, current_timestamp())""")
    seg_flu = '[{"segment": "65 and over", "rule": "age >= 65"}, {"segment": "adult", "rule": "age >= 18"}]'
    seg_lang = '[{"segment": "Spanish", "rule": "preferred_language = \\\'Spanish\\\'"}]'  # \' is an escaped quote in Spark SQL
    campaigns = f"""SELECT * FROM VALUES
      ('CMP-001', 'Birthday greeting (email)', 'recurring', '0 0 8 ? * *', DATE'2026-01-01', NULL, 'email', 'TPL-BIRTHDAY-EMAIL', 'is_birthday_month', NULL, 365, 'campaign', 12, 14, 5000, 'marketing', false, NULL, 'Member Engagement'),
      ('CMP-002', 'Birthday card (Medicare)', 'recurring', '0 0 8 1 * ?', DATE'2026-01-01', NULL, 'mail', 'TPL-BIRTHDAY-CARD', "is_birthday_month AND plan_type = 'Medicare'", NULL, 300, 'campaign', 8, 30, 5000, 'marketing', false, NULL, 'Member Engagement'),
      ('CMP-003', 'Annual wellness visit reminder (Medicare)', 'recurring', '0 0 10 ? * MON-FRI', DATE'2026-01-01', NULL, 'text', 'TPL-AWV-TEXT', "plan_type = 'Medicare' AND days_since_last_encounter > 300", NULL, 30, 'campaign', 8, 7, 3000, 'operational', false, NULL, 'Preventive Care'),
      ('CMP-004', 'New member welcome', 'recurring', '0 0 9 ? * *', DATE'2026-01-01', NULL, 'email', 'TPL-WELCOME-EMAIL', 'days_since_enrollment <= 30', NULL, 365, 'campaign', 12, 14, 5000, 'operational', false, NULL, 'Member Services'),
      ('CMP-005', 'Overdue checkup letter', 'recurring', '0 0 9 ? * WED', DATE'2026-01-01', NULL, 'mail', 'TPL-CHECKUP-MAIL', 'days_since_last_encounter > 365 AND age >= 18', NULL, 90, 'campaign', 8, 30, 4000, 'operational', false, NULL, 'Preventive Care'),
      ('CMP-006', 'Flu shot reminder', 'recurring', '0 30 9 ? * MON-FRI', DATE'2026-09-01', DATE'2026-12-31', 'text', 'TPL-FLU-TEXT', 'age >= 18', '{seg_flu}', 21, 'campaign', 8, 7, 4000, 'operational', false, NULL, 'Preventive Care'),
      ('CMP-007', 'Coverage renewal notice (Medicaid)', 'recurring', '0 0 8 ? * MON', DATE'2026-01-01', NULL, 'mail', 'TPL-RENEWAL-MAIL', "plan_type = 'Medicaid'", NULL, 60, 'campaign', 8, 30, 6000, 'regulatory', true, 'compliance-approver', 'Member Services'),
      ('CMP-008', 'Annual enrollment reminder (Medicare)', 'recurring', '0 0 8 ? * WED', DATE'2026-01-01', NULL, 'mail', 'TPL-AEP-MAIL', "plan_type = 'Medicare'", NULL, 60, 'campaign', 8, 30, 6000, 'regulatory', true, NULL, 'Member Services'),
      ('CMP-009', 'Preventive care reminder (language segments)', 'recurring', '0 0 11 ? * WED', DATE'2026-01-01', NULL, 'email', 'TPL-PREVENTIVE-EMAIL', 'age >= 18 AND days_since_last_encounter > 180', '{seg_lang}', 60, 'campaign', 8, 14, 3000, 'marketing', false, NULL, 'Member Engagement'),
      ('CMP-010', 'Patient portal sign-up', 'recurring', '0 0 12 ? * TUE,WED,THU', DATE'2026-01-01', NULL, 'email', 'TPL-PORTAL-EMAIL', 'age >= 18 AND age < 65', NULL, 120, 'channel', 8, 14, 2500, 'marketing', false, NULL, 'Digital'),
      ('CMP-011', 'Paused: holiday hours notice', 'one_time', '0 0 9 ? * *', DATE'2026-12-20', DATE'2026-12-20', 'email', 'TPL-PORTAL-EMAIL', 'age >= 18', NULL, 0, 'campaign', 8, 3, 5000, 'operational', false, NULL, 'Digital')
      AS t(campaign_id, campaign_name, campaign_type, schedule_cron, start_date, end_date, channel, template_id,
           audience_rule, segmentation_rules, lookback_days, lookback_scope, send_window_hours, expiration_days,
           max_per_run, priority_class, requires_approval, approved_by, owner_team)"""
    # Spark SQL escapes a quote inside a string literal with a backslash; '' would join two literals instead.
    spark.sql(f"""MERGE INTO {T('campaign_config')} t USING ({campaigns}) s ON t.campaign_id = s.campaign_id
                  WHEN NOT MATCHED THEN INSERT (campaign_id, campaign_name, campaign_type, schedule_cron, start_date,
                    end_date, channel, template_id, audience_rule, segmentation_rules, lookback_days, lookback_scope,
                    send_window_hours, expiration_days, max_per_run, priority_class, requires_approval, approved_by,
                    approved_at, owner_team, active, updated_at)
                  VALUES (s.campaign_id, s.campaign_name, s.campaign_type, s.schedule_cron, s.start_date, s.end_date,
                    s.channel, s.template_id, s.audience_rule, s.segmentation_rules, s.lookback_days, s.lookback_scope,
                    s.send_window_hours, s.expiration_days, s.max_per_run, s.priority_class, s.requires_approval,
                    s.approved_by, CASE WHEN s.approved_by IS NOT NULL THEN current_timestamp() END, s.owner_team,
                    s.campaign_id <> 'CMP-011', current_timestamp())""")

print(spark.table(T("campaign_config")).select("campaign_id", "campaign_name", "schedule_cron", "active")
      .toPandas().to_string())
