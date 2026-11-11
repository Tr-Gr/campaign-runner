# Campaign Runner

One config table and one Lakeflow job for every campaign, with safe, exactly-once delivery to the downstream target. Adding a campaign is a row, not a DAG. The shared pipeline stages stay shared; only each campaign's audience rule and optional segmentation live in configuration.

```
campaign_config (one row per campaign)          templates (versioned rows)
          │
          ▼
Campaign Runner job ── plan_today ──► for each due campaign (parallel) ──► egress ──► maintenance ──► summary
                                         │
                                         ▼  run_campaign: one parameterized chain (src/pco_lib.py)
             audience ─► lookbacks ─► target_to_recipient ─► standard_exclusions ─► build_payload
                       ─► segmentation ─► schedule_messages ─► MERGE into message_approved (message_guid)
                                         │
                       message_approved (change data feed on) ──► egress: stage + MERGE in the target
                                         │
                 calendar, run_history, message_summary, run_exceptions ──► dashboard and Genie Agent
```

## How it maps to your current pipeline

| Your pipeline | In this package |
|---|---|
| Airflow DAG per campaign | One `Campaign Runner` job; `plan_today` picks every campaign due today. Airflow can still trigger it (`airflow/campaign_runner_dag.py`). |
| 1. Audience query (campaign-specific) | `audience_rule` in `campaign_config`, a SQL predicate over the `audience_source` view |
| Lookbacks: remove recently sent | `lookbacks()`, using `send_history` and `lookback_days` / `lookback_scope` |
| Target to recipient: resolve contact, dedupe | `target_to_recipient()`, using the `recipient_source` view |
| Exclusions | `standard_exclusions()`: deceased, do-not-contact, `suppression_list` |
| Write JSON payload | `build_payload()`: the template's `payload_fields` as JSON |
| Segmentation (campaign-specific) | `segmentation_rules` in `campaign_config` (optional) |
| Message approved scheduler | `schedule_messages()`: send window and expiration |
| `message_approved` | Same table name, keyed by `message_guid`, written with MERGE |
| Daily delete after 7 days | `maintenance`: deletes only delivered rows after `retention_days` |
| Incremental pull on `created_ts` | `egress`: change data feed from the last delivered version, staging plus MERGE |

## What gets deployed

| Resource | What it does |
|---|---|
| `Campaign Runner - setup` job | Optional synthetic sources, tables, views, sample templates and campaigns. Safe to re-run. |
| `Campaign Runner` job | Daily schedule (paused unless you unpause it). Plan, fan out, egress, maintenance, summary. The summary fails the run if any campaign failed. |
| `Campaign Operations` dashboard | This week's sends, pieces by day and channel, delivery status, run status and exceptions |
| `Campaign Operations` Genie Agent | Marketing self-service over campaign-level views only (no person rows) |

Tables and views are created in `<catalog>.<schema>` with the prefix `camp_` (configurable): `campaign_config`, `templates`, `message_approved`, `send_history`, `suppression_list`, `run_log`, `calendar`, `egress_checkpoint`, `egress_batches`, and the views `run_history`, `message_summary`, `run_exceptions`, plus `audience_source` and `recipient_source` (yours to define; sample versions are created with the sample data).

## Prerequisites

- A Unity Catalog-enabled Azure Databricks workspace with serverless jobs and a SQL warehouse.
- A catalog where you can create a schema, tables and views.
- The [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install), recent enough to support Genie spaces in bundles (upgrade if `databricks bundle validate` reports an unknown resource type).
- Serverless access to PyPI, or a mirror, for `croniter` and `psycopg`.

## Get running in dev

1. In `databricks.yml`, under `targets.dev`, set `workspace.host`, `catalog` and `warehouse_id`.
2. `databricks auth login --host https://<your-dev-workspace>.azuredatabricks.net`
3. `databricks bundle deploy -t dev`. The first deploy can fail on the Genie Agent because its tables don't exist yet; if so, run step 4 and deploy again.
4. `databricks bundle run -t dev campaign_runner_setup` creates synthetic sources and 11 sample campaigns.
5. `databricks bundle run -t dev campaign_runner` (or pass `--params run_date=YYYY-MM-DD`). With `egress_target: none` the messages stay PENDING; set up a target to test delivery.
6. Open the job run (the fan-out shows one run per campaign), `run_history`, the dashboard and the Genie Agent.
7. Run it again: unchanged messages are left alone and nothing is resent.

## Add or change a campaign

- **Add:** copy `src/sql/add_campaign_template.sql`. In Spark SQL, write a quote inside a string as `\'` or wrap the rule in double quotes; two single quotes join two literals instead.
- **Change a template:** add a version row (`src/sql/change_template_version.sql`). Changed messages for the current run date go back to PENDING and are redelivered as updates.
- **Regulatory approval:** set `requires_approval = true`. The campaign appears as `AWAITING_APPROVAL` until `approved_by` is set.
- **Use your data:** replace `audience_source` and `recipient_source` (`src/sql/audience_source_template.sql`), then set `create_sample_data` and `seed_sample_campaigns` to `"false"`.

## Delivery to the target

See `docs/ORACLE_EGRESS.md`. In short: change data feed instead of a timestamp watermark, staging plus MERGE on `message_guid`, mark delivered only after the target commits, delete only delivered rows. `egress_target` is `postgres` (tested), `oracle` (template, not executed) or `none`.

## Staging and prod

1. Create a service principal per environment with the catalog privileges above and permission to use serverless jobs.
2. Set `workspace.host`, `catalog`, `warehouse_id` and the egress variables for `staging` and `prod` in `databricks.yml`.
3. In GitHub, create `staging` and `prod` environments with the variable `DATABRICKS_HOST` and the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. Add required reviewers to `prod`.
4. Pull requests run `validate.yml`; merging to `main` deploys to staging and runs setup; run `deploy.yml` with `target = prod` to promote. The workflows skip themselves until `DATABRICKS_HOST` is set, so nothing runs before you configure them.

## Things to know

- `message_guid` is `sha2(campaign_id|run_date|person_id)`, so a rerun produces the same ids. Unchanged rows are not updated; changed rows are updated and resent; rows that drop out of the audience before delivery are retracted.
- Campaign tasks run in parallel and MERGE into one table; they retry on Delta write conflicts.
- `audience_source` uses `current_date()` for age and birthday month, so backdated runs evaluate attributes as of today.
- The dashboard queries use the default `camp_` prefix; if you change `table_prefix`, update `PREFIX` in `scripts/build_dashboard.py` and rerun it. The Genie Agent definition (`scripts/build_genie_resource.py`) follows the bundle variables automatically.

## Learn more

- [Declarative Automation Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/): [for each tasks](https://learn.microsoft.com/en-us/azure/databricks/jobs/for-each), [triggers](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers)
- [Change data feed](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed)
- [JDBC connection in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/connect/jdbc-connection)
- [Serverless network security](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/)
- [Genie Agents](https://learn.microsoft.com/en-us/azure/databricks/genie-agents/)
- [Airflow and Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/how-to/use-airflow-with-jobs)
