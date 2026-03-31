"""
DAG #4: dbt_transform_mart
==========================
Triggers dbt build to transform staging data into analytics marts.
Runs after the warehouse load DAG succeeds.

Environment:
- dbt project: /opt/airflow/dbt
- Target: dev (postgres-dw)
- Run via: docker compose run --rm dbt
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

with DAG(
    "credit_risk_dbt_marts",
    default_args=default_args,
    description="Run dbt models to transform staging to marts",
    schedule_interval=None,  # Triggered manually or by sensor
    catchup=False,
    tags=["dbt", "warehouse", "marts"],
) as dag:

    # 1. Wait for completion of Task 3.4 (load_to_warehouse)
    # Note: In a real system, you'd use execution_delta or specify the execution_date
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

    # 5. dbt build: Run models and tests
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"docker compose -f {DOCKER_COMPOSE_FILE} run --rm dbt build",
    )

    wait_for_warehouse_load >> dbt_debug >> dbt_seed >> dbt_deps >> dbt_build
