"""
DAG #4: dbt_transform_mart
==========================
Triggers dbt build to transform staging data into analytics marts.
Runs after the warehouse load DAG succeeds, OR runs immediately if triggered manually.

Environment:
- dbt project: /opt/airflow/dbt
- Target: dev (postgres-dw)
- Run via: docker compose run --rm dbt
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os

# Path to docker-compose.yml inside the container
DOCKER_COMPOSE_FILE = "/opt/airflow/docker/docker-compose.yml"

default_args = {
    "owner": "PhanNH",
    "start_date": datetime(2026, 3, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_run_mode(**kwargs):
    """
    Branching logic:
    If the DAG is triggered manually (via UI), skip the sensor and run immediately.
    If triggered by a schedule, wait for the upstream warehouse DAG to finish.
    """
    dag_run = kwargs.get("dag_run")
    # In Airflow, run_type can be a string or an enum depending on the version
    if dag_run and getattr(dag_run.run_type, "value", dag_run.run_type) == "manual":
        return "dbt_debug"
    return "wait_for_warehouse_load"

with DAG(
    "credit_risk_dbt_marts",
    default_args=default_args,
    description="Run dbt models to transform staging to marts",
    schedule_interval=None,  # You can change this to a cron expression if needed
    catchup=False,
    tags=["dbt", "warehouse", "marts"],
    params={
        "data_year": Param(
            2024, 
            type="integer", 
            title="Năm xử lý (Data Year)",
            description="Lọc dữ liệu theo năm để dbt xử lý riêng lẻ (khi chạy bấm tay)"
        ),
    },
) as dag:

    # 0. Check how the DAG was triggered
    branch_task = BranchPythonOperator(
        task_id="check_run_mode",
        python_callable=check_run_mode,
    )

    # 1. Wait for completion of Task 3.4 (load_to_warehouse)
    wait_for_warehouse_load = ExternalTaskSensor(
        task_id="wait_for_warehouse_load",
        external_dag_id="credit_risk_load_warehouse",
        external_task_id="load_to_warehouse",
        check_existence=True,
        timeout=600,
        mode="reschedule",
    )

    # 2. dbt debug: Verify connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt debug",
        trigger_rule=TriggerRule.NONE_FAILED, # Allows running if `wait_for_warehouse_load` is skipped
    )

    # 3. dbt seed: Load lookup CSVs
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt seed",
    )

    # 4. dbt deps: Install any dbt packages (if any)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt deps",
    )

    # 5. dbt build: Run models and tests with year parameter
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="docker compose -f " + DOCKER_COMPOSE_FILE + " run --rm dbt build --vars 'data_year: {{ params.data_year }}'",
    )

    # Define the DAG flow
    branch_task >> wait_for_warehouse_load >> dbt_debug
    branch_task >> dbt_debug
    
    dbt_debug >> dbt_seed >> dbt_deps >> dbt_build
