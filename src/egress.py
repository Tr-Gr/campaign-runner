# Databricks notebook source
# Campaign Runner: egress
#
# Delivers new and changed message_approved rows to the downstream target, exactly once per change:
#   1. Read message_approved's change data feed since the last delivered table version (egress_checkpoint).
#      No created_ts watermark, so late-arriving rows and rows with the same timestamp are never skipped.
#   2. Append the batch to a STAGING table in the target, then MERGE staging into the target on message_guid.
#      Spark JDBC can append or overwrite a table but cannot run MERGE, UPDATE or DELETE, so the MERGE is a
#      separate database step. Rerunning a batch updates the same rows; it never inserts duplicates.
#   3. Only after the target commit: record send_history, mark the rows EGRESSED (status, egressed_at, batch id),
#      record the batch, then advance the checkpoint. A failure anywhere before that leaves the rows PENDING and
#      the next run sends them again, safely.
#
# Targets:
#   postgres  Postgres (tested with a Lakebase database standing in for the production target)
#   oracle    Oracle TEMPLATE (not executed in this package; see oracle_send below and docs/ORACLE_EGRESS.md)
#   none      skip egress (for example while the existing pull process is still in use)
# Network: serverless compute must be able to reach the target. An on-premises Oracle needs private connectivity
# or a firewall allowlist for serverless egress. Confirm this before choosing a push design.

# COMMAND ----------

# MAGIC %run ./pco_lib

# COMMAND ----------

import uuid

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("egress_target", "none")
dbutils.widgets.text("target_table", "message_approved")
dbutils.widgets.text("pg_host", "")
dbutils.widgets.text("pg_database", "")
dbutils.widgets.text("lakebase_endpoint", "")
dbutils.widgets.text("pg_secret_scope", "")
dbutils.widgets.text("oracle_connection", "")
dbutils.widgets.text("oracle_secret_scope", "")
run_id = dbutils.widgets.get("run_id").strip() or "manual"
target = dbutils.widgets.get("egress_target").strip().lower() or "none"
target_table = dbutils.widgets.get("target_table").strip() or "message_approved"
MA = T("message_approved")
started_at = datetime.now()
batch_id = f"egress-{started_at.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
target_name = f"{target}:{target_table}"

if target == "none":
    dbutils.jobs.taskValues.set(key="egress_result", value=json.dumps({"target": "none", "rows_sent": 0}))
    dbutils.notebook.exit("egress_target is none; skipped")

# COMMAND ----------

# 1. What changed since the last delivered version?
to_version = spark.sql(f"DESCRIBE HISTORY {MA} LIMIT 1").first()["version"]
ck = spark.table(T("egress_checkpoint")).where(F.col("target_name") == target_name).collect()
from_version = (ck[0]["last_version"] + 1) if ck else None

snapshot = spark.read.option("versionAsOf", to_version).table(MA).where("egress_status = 'PENDING'")
if from_version is None:
    # First delivery to this target: everything still pending.
    batch = snapshot
elif from_version > to_version:
    batch = snapshot.limit(0)
else:
    changed = (spark.read.option("readChangeFeed", "true").option("startingVersion", from_version)
               .option("endingVersion", to_version).table(MA)
               .where("_change_type IN ('insert', 'update_postimage')")
               .select("message_guid").distinct())
    batch = snapshot.join(changed, "message_guid", "inner")

OUT_COLS = ["message_guid", "campaign_id", "run_date", "person_id", "channel", "recipient_address", "segment",
            "template_id", "template_version", "payload", "content_hash", "send_window_start", "send_window_end",
            "expires_at", "created_ts", "updated_ts"]
batch = batch.select(*OUT_COLS).withColumn("egress_batch_id", F.lit(batch_id))
pdf_rows = batch.count()
print(f"target={target_name} versions {from_version}..{to_version}: {pdf_rows} rows to send")

# COMMAND ----------

# 2. Send: append to staging, then MERGE staging into the target on message_guid.
PG_DDL = """
CREATE TABLE IF NOT EXISTS {t} (
  message_guid text PRIMARY KEY, campaign_id text, run_date date, person_id text, channel text,
  recipient_address text, segment text, template_id text, template_version integer, payload text,
  content_hash text, send_window_start timestamp, send_window_end timestamp, expires_at timestamp,
  created_ts timestamp, updated_ts timestamp, egress_batch_id text, loaded_at timestamp DEFAULT now()
)"""


def pg_credentials():
    host, db = dbutils.widgets.get("pg_host").strip(), dbutils.widgets.get("pg_database").strip()
    endpoint, scope = dbutils.widgets.get("lakebase_endpoint").strip(), dbutils.widgets.get("pg_secret_scope").strip()
    if endpoint:  # Lakebase: short-lived OAuth token for the job identity
        from databricks.sdk import WorkspaceClient
        tok = WorkspaceClient().api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint})
        user, pw = spark.sql("SELECT current_user()").first()[0], tok["token"]
    else:         # any Postgres: user and password from a secret scope
        user, pw = dbutils.secrets.get(scope, "user"), dbutils.secrets.get(scope, "password")
    return host, db, user, pw


def postgres_send(df):
    import psycopg
    host, db, user, pw = pg_credentials()
    stage = f"{target_table}_stage"
    conn_args = dict(host=host, port=5432, dbname=db, user=user, password=pw, sslmode="require", autocommit=True)
    with psycopg.connect(**conn_args) as conn, conn.cursor() as cur:
        cur.execute(PG_DDL.format(t=target_table))
        cur.execute(f"CREATE TABLE IF NOT EXISTS {stage} (LIKE {target_table} INCLUDING DEFAULTS)")
        cur.execute(f"ALTER TABLE {stage} DROP CONSTRAINT IF EXISTS {stage}_pkey")
        cur.execute(f"TRUNCATE {stage}")
    method = "spark_postgresql_append"
    try:
        # Parallel append into staging. Tune numPartitions with the target's DBA: more writers is not always faster.
        (df.write.format("postgresql").option("host", host).option("port", 5432).option("database", db)
           .option("dbtable", stage).option("user", user).option("password", pw)
           .option("numPartitions", 8).option("batchsize", 5000).mode("append").save())
    except Exception as e:  # noqa: BLE001
        print(f"Spark append failed, falling back to COPY: {type(e).__name__}: {str(e)[:300]}")
        method = "psycopg_copy"
        rows = df.toPandas()
        with psycopg.connect(**conn_args) as conn, conn.cursor() as cur:
            cols = ", ".join(rows.columns)
            with cur.copy(f"COPY {stage} ({cols}) FROM STDIN") as cp:
                for r in rows.itertuples(index=False):
                    cp.write_row(tuple(None if (isinstance(v, float) and v != v) else v for v in r))
    cols = OUT_COLS + ["egress_batch_id"]
    sets = ", ".join(f"{c} = s.{c}" for c in cols if c != "message_guid")
    with psycopg.connect(**conn_args) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {stage} s WHERE EXISTS (SELECT 1 FROM {target_table} t "
                    f"WHERE t.message_guid = s.message_guid)")
        updated = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) FROM {stage}")
        sent = cur.fetchone()[0]
        cur.execute(f"""MERGE INTO {target_table} t USING {stage} s ON t.message_guid = s.message_guid
                        WHEN MATCHED THEN UPDATE SET {sets}, loaded_at = now()
                        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})""")
    return {"method": method, "rows_sent": int(sent), "rows_new": int(sent - updated), "rows_updated": int(updated)}


def oracle_send(df):
    """TEMPLATE, not executed in this package. Same shape as postgres_send:
    1. Append to an Oracle staging table through a Unity Catalog JDBC connection (driver JAR in a volume or a
       Maven coordinate, credentials on the connection, USE CONNECTION granted to the job identity).
    2. MERGE staging into the target as a separate database step with python-oracledb (or call a procedure).
    """
    raise NotImplementedError("Oracle egress is a template. Review docs/ORACLE_EGRESS.md, then implement and test.")
    conn_name, scope = dbutils.widgets.get("oracle_connection"), dbutils.widgets.get("oracle_secret_scope")
    (df.write.format("jdbc").option("databricks.connection", conn_name)
       .option("dbtable", f"{target_table.upper()}_STAGE").option("batchsize", 5000).mode("append").save())
    import oracledb  # add "oracledb" to the job environment dependencies
    with oracledb.connect(user=dbutils.secrets.get(scope, "user"), password=dbutils.secrets.get(scope, "password"),
                          dsn=dbutils.secrets.get(scope, "dsn")) as conn, conn.cursor() as cur:
        cur.execute(f"""MERGE INTO {target_table} t USING {target_table}_STAGE s ON (t.message_guid = s.message_guid)
                        WHEN MATCHED THEN UPDATE SET t.payload = s.payload, t.content_hash = s.content_hash,
                          t.updated_ts = s.updated_ts, t.egress_batch_id = s.egress_batch_id
                        WHEN NOT MATCHED THEN INSERT (message_guid, campaign_id, run_date, person_id, channel,
                          recipient_address, payload, content_hash, send_window_start, send_window_end, expires_at,
                          egress_batch_id)
                        VALUES (s.message_guid, s.campaign_id, s.run_date, s.person_id, s.channel,
                          s.recipient_address, s.payload, s.content_hash, s.send_window_start, s.send_window_end,
                          s.expires_at, s.egress_batch_id)""")
        cur.execute(f"TRUNCATE TABLE {target_table}_STAGE")
        conn.commit()


# COMMAND ----------

status, err, res = "SUCCEEDED", None, {"method": target, "rows_sent": 0, "rows_new": 0, "rows_updated": 0}
if pdf_rows > 0:
    try:
        res = postgres_send(batch) if target == "postgres" else oracle_send(batch)
    except Exception as e:  # noqa: BLE001
        status, err = "FAILED", f"{type(e).__name__}: {str(e)[:500]}"

if status == "SUCCEEDED" and pdf_rows > 0:
    # 3. The target has committed: record history and mark exactly the sent versions of these rows delivered.
    batch.select("message_guid", "content_hash", "person_id", "campaign_id", "channel", "run_date") \
         .createOrReplaceTempView("egress_sent")
    with_retry(lambda: spark.sql(f"""
        MERGE INTO {T('send_history')} h USING egress_sent s ON h.message_guid = s.message_guid
        WHEN NOT MATCHED THEN INSERT (message_guid, person_id, campaign_id, channel, sent_date, egress_batch_id, recorded_at)
          VALUES (s.message_guid, s.person_id, s.campaign_id, s.channel, s.run_date, '{batch_id}', current_timestamp())
    """), label="send_history")
    with_retry(lambda: spark.sql(f"""
        MERGE INTO {MA} t USING egress_sent s ON t.message_guid = s.message_guid AND t.content_hash = s.content_hash
        WHEN MATCHED AND t.egress_status = 'PENDING' THEN UPDATE SET
          egress_status = 'EGRESSED', egressed_at = current_timestamp(), egress_batch_id = '{batch_id}'
    """), label="mark_egressed")

finished_at = datetime.now()
spark.createDataFrame([(batch_id, run_id, target_name, res["method"], from_version, int(to_version),
                        int(res["rows_sent"]), int(res["rows_new"]), int(res["rows_updated"]), started_at,
                        finished_at, status, err)], spark.table(T("egress_batches")).schema) \
     .write.mode("append").saveAsTable(T("egress_batches"))

if status == "SUCCEEDED":
    spark.createDataFrame([(target_name, int(to_version), finished_at)], spark.table(T("egress_checkpoint")).schema) \
         .createOrReplaceTempView("ck_new")
    spark.sql(f"""MERGE INTO {T('egress_checkpoint')} c USING ck_new n ON c.target_name = n.target_name
                  WHEN MATCHED THEN UPDATE SET last_version = n.last_version, updated_at = n.updated_at
                  WHEN NOT MATCHED THEN INSERT *""")

result = {"batch_id": batch_id, "target": target_name, "from_version": from_version, "to_version": int(to_version),
          **res, "status": status, "error": err, "seconds": round((finished_at - started_at).total_seconds(), 1)}
print(json.dumps(result, default=str))
dbutils.jobs.taskValues.set(key="egress_result", value=json.dumps(result, default=str))
if status != "SUCCEEDED":
    raise Exception(f"egress failed: {err}")
