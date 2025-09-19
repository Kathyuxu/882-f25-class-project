from prefect import flow, task
import random

# Define the first task
@task(log_prints=True)
def task_one():
    print("Executing Task One")
    fruits = ['Apple', 'Grapes', 'Pear', 'Strawberry']
    fruit = random.choice(fruits)
    return fruit
    

# Define the second task
@task(log_prints=True)
def task_two(fruit):
    print("Executing Task Two")
    print("==================")
    print(f"The randomly selected fruit was: {fruit}")

# Define the flow that calls the tasks
@flow(log_prints=True)
def simple_flow(log):
    fruit = task_one()
    task_two(fruit)

# Run the flow
if __name__ == "__main__":
    simple_flow()