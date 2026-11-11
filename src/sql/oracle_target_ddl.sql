-- Oracle target and staging tables for the oracle egress template (run in Oracle by your DBA).
CREATE TABLE message_approved (
  message_guid VARCHAR2(64) PRIMARY KEY, campaign_id VARCHAR2(64), run_date DATE, person_id VARCHAR2(64),
  channel VARCHAR2(16), recipient_address VARCHAR2(512), segment VARCHAR2(128), template_id VARCHAR2(64),
  template_version NUMBER, payload CLOB, content_hash VARCHAR2(64), send_window_start TIMESTAMP,
  send_window_end TIMESTAMP, expires_at TIMESTAMP, created_ts TIMESTAMP, updated_ts TIMESTAMP,
  egress_batch_id VARCHAR2(64), loaded_at TIMESTAMP DEFAULT SYSTIMESTAMP
);
CREATE TABLE message_approved_stage AS SELECT * FROM message_approved WHERE 1 = 0;
