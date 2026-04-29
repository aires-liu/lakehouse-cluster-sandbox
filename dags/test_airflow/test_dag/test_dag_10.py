from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# Iceberg 最小可行验证 DAG
# 验证内容：Hive Catalog 连通 → 建表 → 写入 → 读取 → 快照 → 时光回溯
# 依赖 Airflow Variables：endpoint_url / aws_access_key_id / aws_secret_access_key

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dag_id = os.path.basename(__file__).replace(".py", "")

# Iceberg 及 AWS SDK v2 JAR 路径（存在于 airflow 容器的 /opt/spark/jars/）
# 通过 --jars 传递，Spark 会将这些 JAR 分发给 executor（spark-worker 容器）
SPARK_JARS_DIR = "/opt/spark/jars"
ICEBERG_JARS = ",".join([
    f"{SPARK_JARS_DIR}/iceberg-spark-runtime-3.3_2.12-1.1.0.jar",
    f"{SPARK_JARS_DIR}/bundle-2.17.230.jar",
    f"{SPARK_JARS_DIR}/url-connection-client-2.17.230.jar",
])

with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False, # 不执行历史任务
    tags=["iceberg", "spark"],
    is_paused_upon_creation=False,
) as dag:

    iceberg_smoke_test = SparkSubmitOperator(
        task_id="iceberg_smoke_test",
        application=f"{project_dir}/spark_jobs/iceberg_conn_test.py",
        conn_id="spark_default",
        jars=ICEBERG_JARS,
        application_args=[
            "--endpoint_url={{ var.value.endpoint_url }}",
            "--aws_access_key_id={{ var.value.aws_access_key_id }}",
            "--aws_secret_access_key={{ var.value.aws_secret_access_key }}",
        ],
        verbose=True,
    )
