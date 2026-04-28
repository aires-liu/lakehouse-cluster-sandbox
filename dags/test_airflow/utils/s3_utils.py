import boto3
from io import BytesIO
import pandas as pd
import re
import json
from tabulate import tabulate
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

class S3Client:
    def __init__(self, endpoint_url, aws_access_key_id, aws_secret_access_key):
        self.client = boto3.client(
            service_name="s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        print("### S3 客户端创建成功")

    def __getattr__(self, attr):
        # 让 S3Client 支持直接调用 boto3.client 的方法
        return getattr(self.client, attr)

    def get_pandas_df_from_csv(self, bucket: str, key: str) -> PandasDataFrame:
        obj = self.client.get_object(Bucket=bucket, Key=key)
        pandas_df = pd.read_csv(BytesIO(obj['Body'].read()))
        print("### 从 MinIO 读取 Pandas DataFrame 成功！")
        print(tabulate(pandas_df, headers='keys', tablefmt='grid', showindex=False))
        return pandas_df

    def write_pandas_df_to_csv(self, pandas_df: PandasDataFrame, bucket: str, key: str) -> None:
        csv_buffer = BytesIO()
        pandas_df.to_csv(csv_buffer, index=False)
        self.client.put_object(Bucket=bucket, Key=key, 
                              Body=csv_buffer.getvalue(), ContentType="text/csv")
        print(f"### Pandas DataFrame 写入 MinIO://{bucket}/{key} 成功！")

    def write_gx_result_to_json(self, gx_result, bucket, key):
        # to_json_dict()将GX校验结果对象转换为一个python字典
        # ensure_ascii=False表示非ASCII字符直接输出原文，indent=2表示每层缩进2个空格
        results_json = json.dumps(gx_result.to_json_dict(), ensure_ascii=False, indent=2)
        self.client.put_object(Bucket=bucket, Key=key, 
                              Body=results_json.encode("utf-8"), ContentType="application/json")
        print(f"### GX 校验结果写入 MinIO://{bucket}/{key} 成功！")

    def get_object_list(self, bucket_name, prefix, pattern='.*', recursive=False):
        """支持正则和递归查询"""
        key_list = []
        kwargs = {"Bucket": bucket_name, "Prefix": prefix, "Delimiter": "/"}
        if recursive:
            kwargs.pop("Delimiter")
        while True:
            response = self.list_objects_v2(**kwargs)
            for obj in response.get('Contents', []):
                key = obj["Key"] 
                if key and re.search(pattern, key):
                    key_list.append(key)
            if response.get("IsTruncated"):
                kwargs["ContinuationToken"] = response.get("NextContinuationToken")
            else:
                break
        return key_list    

class S3SparkSession:
    """
    example:
        ```python
        spark_session = SparkSession.builder.appName("TestSparkApp") \
                        .config("spark.hadoop.fs.s3a.endpoint", Variable.get("endpoint_url")) \
                        .config("spark.hadoop.fs.s3a.access.key", Variable.get("aws_access_key_id")) \
                        .config("spark.hadoop.fs.s3a.secret.key", Variable.get("aws_secret_access_key")) \
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                        .getOrCreate()
        ```
    """
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def __getattr__(self, attr: str) -> object:
        return getattr(self.spark_session, attr)
    
    def get_spark_df_from_csv(self, bucket: str, key: str) -> SparkDataFrame:
        asset_path = f"s3a://{bucket}/{key}"
        spark_df = self.spark_session.read.csv(asset_path, header=True, multiLine=True)
        # multiLine参数用于处理包含换行符的字段，只有字段值被双引号包裹时生效
        # 只有部分字段的部分值被双引号包裹也可以生效
        print("### 从 MinIO 读取 Spark DataFrame 成功！")
        print(spark_df.show())
        return spark_df

    def write_spark_df_to_csv(self, spark_df: SparkDataFrame, bucket: str, key: str) -> None:
        spark_df.write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/{key}")
        print(f"### Spark DataFrame 写入 MinIO://{bucket}/{key} 成功！")


