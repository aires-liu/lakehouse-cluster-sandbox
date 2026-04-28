from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
import os

# 关于分支任务的使用
@task.branch
def choose_branch():
    print("### Branch selector running.")
    second = datetime.now().second
    return "branch_a" if second % 2 == 0 else "branch_b"

@task
def branch_a():
    print("### Branch A running.")

@task
def branch_b():
    print("### Branch B running.")

@task(trigger_rule=TriggerRule.ALL_DONE)
def end_task():
    print("### End task running.")

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
    branch = choose_branch() 
    # 无法直接写成 choose_branch() >> [branch_a(), branch_b()] >> end_task()
    branch >> [branch_a(), branch_b()] >> end_task()

dag = test_dag()