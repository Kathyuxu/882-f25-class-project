import random
from datetime import datetime
from typing import List

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "mapping", "branch"],
)
def random_average_demo():
    """DAG to pick a random value → generate list → map multiply → average → branch."""

    @task
    def pick_random() -> int:
        value = random.randint(1, 10)
        print(f"Picked random value: {value}")
        return value

    @task
    def generate_list(base: int) -> List[int]:
        values = [base + i for i in range(5)]
        print(f"Generated list: {values}")
        return values

    @task
    def multiply_by_two(x: int) -> int:
        result = x * 2
        print(f"Multiplied {x} → {result}")
        return result

    @task
    def collapse_and_average(values: List[int]) -> float:
        avg = sum(values) / len(values)
        print(f"Values: {list(values)}, Average: {avg}")
        return avg

    @task.branch
    def decide(avg: float) -> str:
        # Return the task_id(s) to follow
        return "greater_task" if avg > 4 else "less_task"

    # --- flow ---
    base_val = pick_random()
    numbers = generate_list(base_val)

    # dynamic task mapping
    doubled = multiply_by_two.expand(x=numbers)

    # collapse mapped results into a list and average them
    avg_val = collapse_and_average(doubled)

    # branch
    route = decide(avg_val)

    greater = EmptyOperator(task_id="greater_task")
    less = EmptyOperator(task_id="less_task")

    route >> [greater, less]

random_average_demo()
