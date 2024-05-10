

from __future__ import annotations

import pendulum

import time

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

start_date = pendulum.datetime(2021, 1, 1, tz="UTC")

with DAG(
    dag_id="parent_dag",
    start_date=start_date,
    catchup=False,
    schedule=None,
) as parent_dag:
    delay_python_task: PythonOperator = PythonOperator(task_id="delay_python_task",
                                                       python_callable=lambda: time.sleep(
                                                           300))

with DAG(
    dag_id="external_sensor_child_dag",
    start_date=start_date,
    schedule=None,
    catchup=False,
    tags=["example2"],
) as child_dag:
    sensor_tasks = []
    for i in range(30):
        task = ExternalTaskSensor(
            task_id=f"child_task_{i}",
            external_dag_id=parent_dag.dag_id,
            external_task_id=delay_python_task.task_id,
            allowed_states=["success"],
            failed_states=["failed", "upstream_failed", "skipped"],
            mode="reschedule",
        )
        sensor_tasks.append(task)

    child_task_end = EmptyOperator(task_id="child_task_end")
    sensor_tasks >> child_task_end