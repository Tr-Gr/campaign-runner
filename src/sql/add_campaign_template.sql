-- Add a campaign: one row. The next Campaign Runner run picks it up if it is due. Nothing is deployed.
-- Replace every <...>. Rules are SQL predicates over audience_source columns (literals and operators only).
-- In Spark SQL, write a quote inside a string as \' (two single quotes would join two literals instead),
-- or wrap the rule in double quotes as below.
INSERT INTO <catalog>.<schema>.camp_campaign_config
  (campaign_id, campaign_name, campaign_type, schedule_cron, start_date, end_date, channel, template_id,
   audience_rule, segmentation_rules, lookback_days, lookback_scope, send_window_hours, expiration_days,
   max_per_run, priority_class, requires_approval, approved_by, approved_at, owner_team, active, updated_at)
VALUES
  ('<CMP-nnn>', '<campaign name marketing uses>',
   'recurring',                  -- or one_time (sends once on start_date)
   '0 0 9 ? * MON-FRI',          -- Quartz cron, Central time: 9:00 on weekdays
   DATE'<start>', NULL,          -- end_date NULL = no end
   'email',                      -- email | text | mail
   '<TEMPLATE-ID>',
   "plan_type = 'Medicare' AND age >= 65",
   NULL,                         -- or '[{"segment": "65 and over", "rule": "age >= 65"}]'
   30, 'campaign',               -- do not resend this campaign to the same person within 30 days
   8, 7, 5000,                   -- send window hours, expiration days, max messages per run
   'operational',                -- regulatory | operational | marketing
   false, NULL, NULL,            -- regulatory: requires_approval = true; the campaign waits until approved_by is set
   '<owner team>', true, current_timestamp());

-- Approve a regulatory campaign:
-- UPDATE <catalog>.<schema>.camp_campaign_config SET approved_by = '<approver>', approved_at = current_timestamp() WHERE campaign_id = '<CMP-nnn>';
-- Pause a campaign:
-- UPDATE <catalog>.<schema>.camp_campaign_config SET active = false WHERE campaign_id = '<CMP-nnn>';
