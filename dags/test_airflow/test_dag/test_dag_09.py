from datetime import datetime
from airflow import DAG
import os
import sys
import pandas as pd
from airflow.models import Variable
from great_expectations.core import ExpectationSuiteValidationResult
dags_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(dags_dir)
from test_gx import custom_expectations
from test_airflow.custom_operator.validate_dataframe import GXValidateDataFrameOperator
from test_airflow.utils.s3_utils import S3Client

# 测试gx与airflow集成的自定义操作符
s3_client_instance = None
def get_s3_client() -> S3Client:
    global s3_client_instance
    # 声明这里使用的是模块级的全局变量
    # 使用全局变量和实例来避免dag加载时就实例化客户端，同时避免两个方法重复创建客户端
    # 并且把客户端的创建和GXValidateDataFrameOperator解耦
    if s3_client_instance is None:
        # Jinja模板只能在Operator中使用，这种情况下用Variable最合适
        s3_client_instance = S3Client(
            endpoint_url=Variable.get("endpoint_url"),
            aws_access_key_id=Variable.get("aws_access_key_id"),
            aws_secret_access_key=Variable.get("aws_secret_access_key")
        )
    return s3_client_instance

def get_s3_df(bucket: str, key: str) -> pd.DataFrame:
    s3_client = get_s3_client()
    return s3_client.get_pandas_df_from_csv(bucket, key)

def write_to_s3(gx_result: ExpectationSuiteValidationResult, bucket: str, key: str) -> None:
    """自定义writer方法的时第一个参数在operator里面默认会接收到gx_result"""
    s3_client = get_s3_client()
    s3_client.write_gx_result_to_json(gx_result, bucket, key)

dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "gx", "custom operator"],
    params={"run_timestamp": datetime.now().strftime('%Y%m%d%H%M%S')},
    is_paused_upon_creation=False
) as dag:

    expectation_config = [
        {
            "expectation_type": "expect_column_values_to_be_in_set", 
            "kwargs": {
                "column": "payment_method", 
                "value_set": ["credit_card", "paypal", "bank_transfer", "cash"]
            }
        },
        {
            "expectation_type": "expect_column_values_to_meet_date_condition", 
            "kwargs": {
                "column": "order_date", 
                "date": "2024-09-08", 
                "operator": ">=",
                "meta": {"description": "SYS0001"},
                "row_condition": "index > -1",
                "condition_parser": "pandas"
            }
        }
    ]

    gx_validate = GXValidateDataFrameOperator(
        task_id="gx_validate",
        dataframe_loader=get_s3_df,
        result_writer=write_to_s3,
        expectation_config=expectation_config,
        result_format={"result_format": "COMPLETE"},
        loader_kwargs={
            "bucket": "test-bucket",
            "key": "input/test_data_02.csv"
        },
        writer_kwargs={
            "bucket": "test-bucket",
            "key": "output/gx_result_test_data_02_spark_{{ params['run_timestamp'] }}.json"
        }
    )