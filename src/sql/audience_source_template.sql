-- Replace the sample views with definitions over your own tables. Campaign rules can use any column of
-- audience_source; recipient_source supplies contact points and flags for target_to_recipient and exclusions.
CREATE OR REPLACE VIEW <catalog>.<schema>.camp_audience_source AS
SELECT p.person_id, p.first_name, p.gender, p.preferred_language,
       CAST(floor(months_between(current_date(), p.birth_date) / 12) AS INT) AS age,
       month(p.birth_date) AS birth_month,
       month(p.birth_date) = month(current_date()) AS is_birthday_month,
       e.plan_type, e.line_of_business, e.group_id, e.eligibility_start,
       datediff(current_date(), e.eligibility_start) AS days_since_enrollment,
       n.clinic_name, n.network_name
       -- add any attribute campaigns filter on, for example last encounter date or open care gaps
FROM <your person identity table> p
JOIN <your eligible members table> e ON p.person_id = e.person_id
JOIN <your primary active medical member table> a ON p.person_id = a.person_id
LEFT JOIN <your delivery network table> n ON p.person_id = n.person_id;

CREATE OR REPLACE VIEW <catalog>.<schema>.camp_recipient_source AS
SELECT person_id, email, mobile_phone, mailing_address, email_opt_in, sms_opt_in, mail_opt_in,
       do_not_contact, deceased_flag
FROM <your contact points table>;
