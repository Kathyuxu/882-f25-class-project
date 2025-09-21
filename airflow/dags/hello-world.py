from airflow.decorators import dag, task
from datetime import datetime
import random

@dag(
    schedule=None,                # run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["BA882", "tuts"]
)
def hello_world_882():

    @task
    def task_one():
        print("Executing Task One")
        fruits = ['Apple', 'Grapes', 'Pear', 'Strawberry']
        fruit = random.choice(fruits)
        return fruit
    
    @task
    def task_two(fruit):
        print("Executing Task Two")
        print("==================")
        print(f"The randomly selected fruit was: {fruit}")
    
    output_t1 = task_one()
    task_two(output_t1)


hello_world_882()