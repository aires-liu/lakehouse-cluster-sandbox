from airflow import DAG
import os
from datetime import datetime
from airflow.operators.python import PythonOperator
import sys
dags_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(dags_dir)
from test_airflow.utils.s3_utils import S3Client

# 测试用airflow容器取读取minio容器中的数据

def load_s3_dataframe(endpoint_url, aws_access_key_id, aws_secret_access_key, bucket, key):
    s3_client = S3Client(endpoint_url, aws_access_key_id, aws_secret_access_key)
    pandas_df = s3_client.get_pandas_df_from_csv(bucket, key)
    return pandas_df

# 方法一：使用Jinja模板进行变量获取
dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "minio"],
    is_paused_upon_creation=False
) as dag:
    # 使用airflow的jinja模板来获取敏感变量
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=load_s3_dataframe,
        op_kwargs={
            "endpoint_url": "{{ var.value.endpoint_url }}",
            "aws_access_key_id": "{{ var.value.aws_access_key_id }}",
            "aws_secret_access_key": "{{ var.value.aws_secret_access_key }}",
            "bucket": "test-bucket",
            "key": "input/test_data_02.csv"
        },
        dag=dag,
    )


# 方法二：使用Airflow的Variable来获取敏感变量
# from airflow.models import Variable

# dag_id = os.path.basename(__file__).replace(".py", "")
# with DAG(
#     dag_id=dag_id,
#     start_date=datetime(2023, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["test", "minio"],
#     is_paused_upon_creation=False
# ) as dag:

#     get_data = PythonOperator(
#         task_id="get_data",
#         python_callable=load_s3_dataframe,
#         op_kwargs={
#             "endpoint_url": Variable.get('endpoint_url'),
#             "aws_access_key_id": Variable.get('aws_access_key_id'),
#             "aws_secret_access_key": Variable.get('aws_secret_access_key'),
#             "bucket": "test-bucket",
#             "key": "input/test_data_02.csv"
#         },
#         dag=dag,
#     )
