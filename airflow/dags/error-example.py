# dags/task_error_demo.py

from datetime import datetime
from airflow.decorators import dag, task

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "bug"],
)
def task_error_demo():
    """DAG with an intentional error inside a task."""

    @task
    def broken_task():
        # ❌ BUG: division by zero
        value = 1 / 0
        return value  #return, not returns

    broken_task()

task_error_demo()
