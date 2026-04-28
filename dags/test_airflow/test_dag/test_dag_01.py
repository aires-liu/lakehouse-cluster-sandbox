from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# 一个最简单的DAG示例，包含三个任务：
# 1. start_task: 开始任务，推送一条消息到XCom
# 2. process_task: 处理任务，从XCom中拉取消息并打印
# 3. end_task: 结束任务，打印结束信息

def start_task(**context):
    print("### Start task running.")
    context['ti'].xcom_push(key='msg', value='Hello from start_task!')

def process_task(**context):
    msg = context['ti'].xcom_pull(key='msg', task_ids='start_task')
    print(f"### Process task received message: {msg}")

def end_task():
    print("### End task running.")

dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
    is_paused_upon_creation=False
) as dag:
    t1 = PythonOperator(
        task_id="start_task",
        python_callable=start_task,
        provide_context=True,
    )
    t2 = PythonOperator(
        task_id="process_task",
        python_callable=process_task,
        provide_context=True,
    )
    t3 = PythonOperator(
        task_id="end_task",
        python_callable=end_task,
    )

    t1 >> t2 >> t3