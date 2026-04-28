from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import sys
dags_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(dags_dir)
from test_airflow.utils.s3_utils import S3Client

# 通过第三方存储MinIO实现跨DAG间的通信问题
def get_s3_object_content(bucket, key):
    print(f"bucket={bucket}, key={key}")
    endpoint_url=Variable.get('endpoint_url')
    aws_access_key_id=Variable.get('aws_access_key_id')
    aws_secret_access_key=Variable.get('aws_secret_access_key')
    s3_client = S3Client(endpoint_url, aws_access_key_id, aws_secret_access_key)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    print(content)

dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
    params={"run_timestamp": datetime.now().strftime('%Y%m%d%H%M%S')},
    is_paused_upon_creation=False
) as dag:

    # 触发test_06_dag提交一个spark任务读取MinIO中的数据
    # 并用GX进行验证，验证结果写入MinIO中，结果文件中带有run_timestamp
    trigger_task = TriggerDagRunOperator(
        task_id="trigger_test_dag_06",
        trigger_dag_id="test_dag_06",
        wait_for_completion=True,
        poke_interval=10,
        conf={"run_timestamp": "{{ params['run_timestamp'] }}"},
    )

    # 通过run_timestamp获取MinIO中GX验证结果
    get_s3_object = PythonOperator(
        task_id="get_s3_object",
        python_callable=get_s3_object_content,
        op_kwargs={
            "bucket": "test-bucket",
            "key": "output/gx_result_test_data_02_spark_{{ params['run_timestamp'] }}.json"
        }
    )

    trigger_task >> get_s3_object

    
