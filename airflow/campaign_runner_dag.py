"""Optional: keep Airflow as the scheduler and let it trigger the one Campaign Runner job.

One DAG replaces the per-campaign DAGs: the job reads campaign_config and runs every campaign that is due.
Needs apache-airflow-providers-databricks. Authenticate as a Databricks service principal with OAuth: store the
client id and secret in an Airflow connection named "databricks_campaigns" (type Databricks, host = workspace URL,
login = client id, password = client secret, extra = {"auth_type": "oauth"}). Give the service principal
CAN_MANAGE_RUN on the job. If you schedule in Databricks instead, unpause the job's own schedule and drop this DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

CAMPAIGN_RUNNER_JOB_ID = 0  # set to the job id shown by: databricks bundle summary -t prod

with DAG(
    dag_id="campaign_runner_daily",
    description="Trigger the Databricks Campaign Runner job for the run date",
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",           # 06:00 daily; set the DAG timezone to America/Chicago
    catchup=False,
    default_args={"owner": "campaigns", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["campaigns", "databricks"],
) as dag:
    DatabricksRunNowOperator(
        task_id="run_campaign_runner",
        databricks_conn_id="databricks_campaigns",
        job_id=CAMPAIGN_RUNNER_JOB_ID,
        job_parameters={"run_date": "{{ ds }}"},   # the job plans the campaigns due on this date
        databricks_retry_limit=3,
        wait_for_termination=True,
    )
