-- Delivery checks for operators.
-- Undelivered after the send window (also shown as NOT_DELIVERED in run_exceptions):
SELECT campaign_name, run_date, count(*) AS pending FROM <catalog>.<schema>.camp_message_approved
WHERE egress_status = 'PENDING' AND send_window_end < current_timestamp() GROUP BY ALL;
-- Recent egress batches:
SELECT * FROM <catalog>.<schema>.camp_egress_batches ORDER BY started_at DESC LIMIT 20;
-- Last delivered table version per target:
SELECT * FROM <catalog>.<schema>.camp_egress_checkpoint;
