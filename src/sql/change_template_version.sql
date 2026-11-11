-- Change a template: add a new version row. Every campaign that uses it picks it up on its next run, and
-- messages for the current run date that changed go back to PENDING and are re-delivered as updates.
INSERT INTO <catalog>.<schema>.camp_templates
VALUES ('<TEMPLATE-ID>', <next version>, '<template name>', 'email', 'first_name,clinic_name', true,
        '<what changed>', current_timestamp());
