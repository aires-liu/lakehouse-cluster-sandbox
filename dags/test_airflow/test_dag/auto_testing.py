import os
import re
import sys
import subprocess
import logging
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

current_dir = os.path.dirname(os.path.realpath(__file__))
pattern = re.compile(r"^test_dag_\d{2}.py$")
test_dags = [f[:-3] for f in os.listdir(current_dir) if pattern.fullmatch(f)]


def run_gx_auto_testing_script():
    logger = logging.getLogger("airflow.task")
    gx_auto_testing_module = os.path.join(current_dir, "..", "..", "test_gx", "test_scripts", "auto_testing.py")
    script_path = os.path.abspath(gx_auto_testing_module)

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    if process.stdout is not None:
        for line in process.stdout:
            logger.info(line.rstrip("\n"))

    return_code = process.wait()
    if return_code != 0:
        raise AirflowException(f"gx auto testing failed with exit code {return_code}")


with DAG(
    dag_id="auto_testing",
    start_date=datetime(2023, 1, 1), # 默认为当前时间
    schedule_interval=None, # 不设置定时调度只能手动触发
    catchup=False, # 不对历史任务进行补跑
    tags=["test"],
    max_active_tasks=3,
    is_paused_upon_creation=False # 创建后不暂停可以直接运行，避免无法触发新增的DAG
) as dag:

    trigger_tasks = []
    for test_dag in test_dags:
        conf = {"run_timestamp": datetime.now().strftime('%Y%m%d%H%M%S')} \
               if test_dag == "test_dag_06" or test_dag == "test_dag_08" else None
        trigger_task = TriggerDagRunOperator(
            task_id=f"{test_dag}",
            trigger_dag_id=test_dag,
            wait_for_completion=True,
            poke_interval=10,
            conf=conf
        )
        trigger_tasks.append(trigger_task)

    test_gx = PythonOperator(
        task_id="gx_auto_testing",
        python_callable=run_gx_auto_testing_script
    )

    trigger_tasks >> test_gx
