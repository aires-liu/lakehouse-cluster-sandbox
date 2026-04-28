from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.utils.session import provide_session
import os

# 通过查询XCom实现跨DAG间的通信问题
@provide_session
def _get_xcom_value(dag_id, task_id, run_id, session=None):
    # session必须显式指定并且为最后一位参数，这意味着不能使用*args或**kwargs形式的参数传递
    record = session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.task_id == task_id,
        XCom.run_id == run_id,
        XCom.key == 'return_value'  # 默认的返回值key，同一dag_id、task_id、run_id下可能有多个XCom记录
    ).order_by(XCom.timestamp.desc()).first() 
    # 这里的timestamp测试的是mysql数据库，其他数据库可能有不同
    return record.value if record else None

def get_xcom_from_triggered_dag(**context):
    trigger_run_id = context['ti'].xcom_pull(task_ids='trigger_test_dag_03', key='trigger_run_id')
    return _get_xcom_value(dag_id='test_dag_03', task_id='choose_branch', run_id=trigger_run_id)

dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
    is_paused_upon_creation=False
) as dag:
    
    trigger_task = TriggerDagRunOperator(
        task_id="trigger_test_dag_03",
        trigger_dag_id="test_dag_03",
        wait_for_completion=True, # 等待触发的dag执行完再进行下一步
        poke_interval=10 # 轮询时间修改为10秒，默认60秒
    )

    get_xcom_task = PythonOperator(
        task_id="get_xcom_from_triggered_dag",
        python_callable=get_xcom_from_triggered_dag,
        provide_context=True
    )

    trigger_task >> get_xcom_task