# Oracle egress: options and fixes

## What changes from an incremental pull

Today a Python process pulls new `message_approved` rows into Oracle using a watermark on `created_ts` and `message_guid`, and a daily job deletes rows older than 7 days. Two risks come with that design:

| Risk | What happens | Fix in this package |
|---|---|---|
| Late or tied timestamps | A row committed after the pull read its watermark, or rows that share a timestamp, can be skipped or read twice. | Egress reads Delta **change data feed** from the last delivered table version (`egress_checkpoint`). Every insert and update is seen exactly once, by commit version, not by clock. |
| Age-based delete | If the pull is down longer than 7 days, messages are deleted before Oracle ever receives them, and nothing errors. | `maintenance` deletes only rows that are **EGRESSED** and were delivered more than `retention_days` ago. Undelivered rows stay and appear as `NOT_DELIVERED` in `run_exceptions`. |
| Reruns | A retry can insert duplicates. | The target is updated with a **MERGE on `message_guid`**, so resending a batch updates the same rows. Unchanged messages are never resent. |

Lookbacks read `send_history`, which is written at delivery and kept for `history_retention_days` (400 by default), so cleaning up `message_approved` never weakens "don't resend within N days".

## Option A: push from Databricks (recommended to test first)

1. Create a Unity Catalog **JDBC connection** for Oracle with the Oracle driver (`ojdbc11.jar` in a volume, or a Maven coordinate) and credentials on the connection. Grant `USE CONNECTION` to the job's service principal.
2. The egress task appends the batch to `MESSAGE_APPROVED_STAGE` in parallel with `format("jdbc").option("databricks.connection", ...)`.
3. A separate database step MERGEs staging into `MESSAGE_APPROVED` on `message_guid`. Spark JDBC can append to or overwrite a table, but it cannot run MERGE, UPDATE or DELETE, so this step uses `python-oracledb` (or calls an Oracle procedure).
4. Only after the MERGE commits are the rows marked delivered in Databricks.

The code path is `oracle_send()` in `src/egress.py`. It is a **template and is not executed** in this package; the same shape is tested end to end with `postgres_send()`. Table DDL is in `src/sql/oracle_target_ddl.sql`.

Measure in non-production before choosing: rows per second at a few `numPartitions` settings (tune with your DBA, because too many writers can slow Oracle down), MERGE time, rerun behaviour, and session or lock impact on Oracle.

## Option B: Oracle pulls from an OpenSharing share

If your Oracle database is **Oracle Autonomous Database**, it can read a Delta Sharing (OpenSharing) share through `DBMS_SHARE`, so Oracle pulls and there is no copy job on the Databricks side. Check before choosing:

- Oracle does not support shared Delta tables with **deletion vectors**. `message_approved` has deletion vectors on, so share a separate table without them (for example a table the egress step maintains) rather than `message_approved` itself.
- A private-endpoint Oracle database needs outbound access to the sharing endpoint and the storage that serves the files.
- For Oracle Database outside Autonomous Database (including on-premises), confirm support with Oracle. This package has not verified it.

## Option C: keep the existing script, governed

Run the current Python script as a task in the Campaign Runner job (`egress_target: none`, plus your own task), so it gets run history, alerts and retries. Point it at the change data feed and `egress_checkpoint` instead of the `created_ts` watermark, and switch the cleanup to delete only delivered rows. This is the smallest change.

## The network question (answer first)

Serverless compute has to reach Oracle for Option A. For an on-premises or private Oracle, set up **private connectivity** for serverless or allowlist the serverless egress addresses on your firewall. See the Azure Databricks docs on serverless network security. If Oracle cannot be reached from Azure, use Option C on compute that can, or Option B where Oracle pulls.
