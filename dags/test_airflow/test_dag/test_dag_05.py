from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

# 测试airflow容器提交任务到spark集群中，spark容器中读取minio容器中的数据后进行校验
# 使用Variable来获取敏感变量
def spark_run_job(**context):
    endpoint_url=Variable.get('endpoint_url')
    aws_access_key_id=Variable.get('aws_access_key_id')
    aws_secret_access_key=Variable.get('aws_secret_access_key')
    spark_job = SparkSubmitOperator(
        task_id="spark_job", # 注意不要和调用这个方法的task名字一样
        application=f"{project_dir}/spark_jobs/gx_validate_minio.py",
        conn_id="spark_default",
        verbose=True, # 控制 Spark 提交任务时的日志输出详细程度
        application_args=[
            "--endpoint_url", endpoint_url,
            "--aws_access_key_id", aws_access_key_id,
            "--aws_secret_access_key", aws_secret_access_key
        ],
        dag=dag,
    )
    spark_job.execute(context) # 直接调用 Operator 的底层方法
    # conn_id如果在airflow的connection中设置好了就不需要在conf参数里写
    # 默认以"spark.submit.deployMode": "client"提交
    # python环境下无法使用cluster模式，因为python是解释型语言
    # 以standalone部署的spark集群中，driver运行在airflow容器上
    # 因此解析时的依赖库需要在airflow容器环境中安装，提交任务时在airflow中解析
    # spark集群的环境中不需要安装依赖库

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dag_id = os.path.basename(__file__).replace(".py", "")
with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "gx", "spark", "minio"],
    is_paused_upon_creation=False
) as dag:
    
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=spark_run_job,
        provide_context=True
    )
