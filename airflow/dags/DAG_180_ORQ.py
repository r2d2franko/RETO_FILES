
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    "DAG_180_ORQ",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2,"email_on_failure": True,"email": "noreply@astronomer.io"},
    # [END default_args]
    description="DAG RETO PRQ",
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 12, tz="UTC"),
    catchup=False,
    tags=["RETO"]
) as dag:
    trigger_extract = TriggerDagRunOperator(
    task_id="EXTRACCION",
    trigger_dag_id="DAG_180_EXTRACCION_RETO",  # Ensure this equals the dag_id of the DAG to trigger
    conf={"message": "Hello World"},
    )

