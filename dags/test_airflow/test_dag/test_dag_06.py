from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# 基于test_dag_04的基础上做一些优化和更复杂的测试
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "gx", "spark", "minio"],
    params={"run_timestamp": f"{datetime.now().strftime('%Y%m%d%H%M')}"},
    is_paused_upon_creation=False
) as dag:
    
    validate_data = SparkSubmitOperator(
        task_id="validate_data", 
        application=f"{project_dir}/spark_jobs/gx_validate_minio.py",
        conn_id="spark_default",
        application_args=[
            "--endpoint_url={{ var.value.endpoint_url }}", 
            "--aws_access_key_id={{ var.value.aws_access_key_id }}", 
            "--aws_secret_access_key={{ var.value.aws_secret_access_key }}",
            "--run_timestamp={{ dag_run.conf.get('run_timestamp', params.run_timestamp) if dag_run else params.run_timestamp }}"
        ], # 这里用命令行参数的形式传递，在任务脚本里用argparse来解析，可以避免参数顺序的问题
        # 同时有效避免提交任务到spark集群时，日志打印有可能泄露敏感变量的问题
        verbose=True # 用于控制 SparkSubmitOperator 执行时的日志输出级别，默认False，输出简要日志
    )
