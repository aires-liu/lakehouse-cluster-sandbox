from airflow.decorators import dag, task
from datetime import datetime
import os

# @task用于装饰一个函数把它变成Airflow任务
# 支持参数传递和返回值（自动用XCom传递）
@task
def start_task():
    print("### Start task running.")
    return "Hello from start_task!"

@task
def process_task(msg):
    print(f"### Process task received message: {msg}")

@task
def end_task():
    print("### End task running.")

# @dag用于装饰一个函数把它变成一个DAG对象
# 可以在函数内定义任务和依赖关系，并且通过参数设置DAG的属性
dag_id = os.path.basename(__file__).replace(".py", "")
@dag(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
    is_paused_upon_creation=False
)
def test_dag():
    msg = start_task()
    processed = process_task(msg)
    processed >> end_task()

dag = test_dag()