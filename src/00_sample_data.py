# Databricks notebook source
# Campaign Runner: 00_sample_data (optional; part of the setup job)
# Creates SYNTHETIC source tables shaped like a typical member-communications setup, plus the two views every
# campaign reads from. Nothing here reads real data. Set create_sample_data = false once your own views exist.
#
# Sample sources (in <catalog>.<schema>, prefixed):
#   src_person_identity, src_eligible_members, src_primary_active_medical_member, src_encounters,
#   src_delivery_network, src_contact_points
# Views the pipeline reads (replace these two with your own definitions over your real tables):
#   audience_source   one row per person with the attributes campaign rules may use
#   recipient_source  one row per person with contact points, opt-ins and do-not-contact flags

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "campaigns")
dbutils.widgets.text("table_prefix", "camp_")
dbutils.widgets.text("create_sample_data", "true")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
assert CAT and SCH, "Set the catalog and schema parameters"
if dbutils.widgets.get("create_sample_data").strip().lower() != "true":
    dbutils.notebook.exit("create_sample_data is false; skipped")
T = lambda n: f"{CAT}.{SCH}.{P}{n}"

# COMMAND ----------

import numpy as np
import pandas as pd

rng = np.random.default_rng(7)
N = 20000
today = pd.Timestamp.today().normalize()
ids = [f"P{i:07d}" for i in range(N)]
first = np.array(["Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery", "Quinn",
                  "Maria", "Jose", "Linh", "Amina", "Omar", "Grace", "Henry", "Olivia", "Noah", "Emma"])
last = np.array(["Lee", "Garcia", "Nguyen", "Smith", "Johnson", "Brown", "Ali", "Kim", "Patel", "Lopez"])
age = rng.integers(0, 95, N)
birth = [today - pd.DateOffset(years=int(a)) - pd.Timedelta(days=int(rng.integers(0, 365))) for a in age]
person = pd.DataFrame({
    "person_id": ids,
    "first_name": rng.choice(first, N), "last_name": rng.choice(last, N),
    "birth_date": [b.date() for b in birth],
    "gender": rng.choice(["F", "M", "X"], N, p=[0.51, 0.48, 0.01]),
    "preferred_language": rng.choice(["English", "Spanish", "Somali", "Hmong", "Vietnamese"], N,
                                     p=[0.82, 0.09, 0.04, 0.03, 0.02]),
    "deceased_flag": rng.random(N) < 0.005,
})
plan = np.where(age >= 65, "Medicare", rng.choice(["Commercial", "Medicaid"], N, p=[0.78, 0.22]))
elig_start = [today - pd.Timedelta(days=int(d)) for d in rng.integers(1, 2000, N)]
eligible = pd.DataFrame({
    "person_id": ids, "member_id": [f"M{i:08d}" for i in range(N)], "plan_type": plan,
    "line_of_business": np.where(plan == "Commercial", rng.choice(["Group", "Individual"], N, p=[0.8, 0.2]), plan),
    "group_id": [f"G{g:03d}" for g in rng.integers(100, 140, N)],
    "eligibility_start": [d.date() for d in elig_start],
    "eligibility_end": [(today - pd.Timedelta(days=30)).date() if x < 0.02 else None for x in rng.random(N)],
})
primary_active = pd.DataFrame({"person_id": [p for p, keep in zip(ids, rng.random(N) < 0.93) if keep],
                               "active_flag": True})
clinics = ["Northside Clinic", "Lakeview Clinic", "Riverside Clinic", "Westfield Clinic", "Eastgate Clinic",
           "Southpoint Clinic"]
network = pd.DataFrame({"person_id": ids, "clinic_name": rng.choice(clinics, N),
                        "network_name": rng.choice(["Primary Network", "Partner Network"], N, p=[0.85, 0.15])})
n_enc = rng.integers(0, 6, N)
enc_rows = []
for pid, k in zip(ids, n_enc):
    for _ in range(int(k)):
        enc_rows.append((pid, (today - pd.Timedelta(days=int(rng.integers(1, 900)))).date(),
                         rng.choice(["office", "telehealth", "urgent_care", "preventive"])))
encounters = pd.DataFrame(enc_rows, columns=["person_id", "encounter_date", "encounter_type"])
has_email, has_mobile = rng.random(N) < 0.78, rng.random(N) < 0.70
contacts = pd.DataFrame({
    "person_id": ids,
    "email": [f"{p.lower()}@example.org" if h else None for p, h in zip(ids, has_email)],
    "mobile_phone": [f"555-01{int(x):02d}-{i % 10000:04d}" if h else None
                     for i, (x, h) in enumerate(zip(rng.integers(0, 99, N), has_mobile))],
    "mailing_address": [f"{int(n)} Example St, Unit {i % 50}" for i, n in enumerate(rng.integers(1, 9999, N))],
    "email_opt_in": rng.random(N) < 0.92, "sms_opt_in": rng.random(N) < 0.85, "mail_opt_in": rng.random(N) < 0.98,
    "do_not_contact": rng.random(N) < 0.01,
})

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for name, df in [("src_person_identity", person), ("src_eligible_members", eligible),
                 ("src_primary_active_medical_member", primary_active), ("src_delivery_network", network),
                 ("src_encounters", encounters), ("src_contact_points", contacts)]:
    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    spark.sql(f"COMMENT ON TABLE {T(name)} IS 'SYNTHETIC sample data for Campaign Runner'")
    print(f"{T(name)}: {len(df):,} rows")

# a few people on the global suppression list (the table itself is created by 00_setup)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('suppression_list')} (person_id STRING, reason STRING, added_at TIMESTAMP)""")
spark.sql(f"DELETE FROM {T('suppression_list')} WHERE reason = 'sample'")
spark.sql(f"""INSERT INTO {T('suppression_list')}
              SELECT person_id, 'sample', current_timestamp() FROM {T('src_person_identity')} WHERE rand(11) < 0.005""")

# COMMAND ----------

# The two views every campaign reads. Replace these definitions with joins over your own tables;
# campaign rules may use any column of audience_source.
spark.sql(f"""
CREATE OR REPLACE VIEW {T('audience_source')}
COMMENT 'One row per active eligible member with the attributes campaign rules may reference'
AS
WITH last_enc AS (
  SELECT person_id, max(encounter_date) AS last_encounter_date FROM {T('src_encounters')} GROUP BY person_id
)
SELECT p.person_id, p.first_name, p.gender, p.preferred_language,
       CAST(floor(months_between(current_date(), p.birth_date) / 12) AS INT) AS age,
       month(p.birth_date) AS birth_month,
       month(p.birth_date) = month(current_date()) AS is_birthday_month,
       e.plan_type, e.line_of_business, e.group_id, e.eligibility_start,
       datediff(current_date(), e.eligibility_start) AS days_since_enrollment,
       n.clinic_name, n.network_name,
       l.last_encounter_date,
       coalesce(datediff(current_date(), l.last_encounter_date), 9999) AS days_since_last_encounter
FROM {T('src_person_identity')} p
JOIN {T('src_eligible_members')} e ON p.person_id = e.person_id
JOIN {T('src_primary_active_medical_member')} a ON p.person_id = a.person_id AND a.active_flag
LEFT JOIN {T('src_delivery_network')} n ON p.person_id = n.person_id
LEFT JOIN last_enc l ON p.person_id = l.person_id
WHERE e.eligibility_end IS NULL OR e.eligibility_end >= current_date()
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {T('recipient_source')}
COMMENT 'One row per person: contact points, opt-ins and do-not-contact flags used by target_to_recipient'
AS
SELECT c.person_id, c.email, c.mobile_phone, c.mailing_address, c.email_opt_in, c.sms_opt_in, c.mail_opt_in,
       c.do_not_contact, p.deceased_flag
FROM {T('src_contact_points')} c JOIN {T('src_person_identity')} p ON c.person_id = p.person_id
""")
print(spark.table(T("audience_source")).count(), "people in audience_source")
