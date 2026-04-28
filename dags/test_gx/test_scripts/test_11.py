import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
import sys
import pandas as pd
from pyspark.sql import SparkSession
    
# test_09中通过row_condition解决同一期望对同一列校验的覆盖问题
# 实际应用中row_condition相当于承担了选取校验行和避免覆盖的功能
# 这里通过改写具体期望子类的domain_keys实现同样的功能
# 从这里的实现推测GX内部判断是否重复验证的result应该是通过metric和domain来判断的

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
dags_dir = os.path.dirname(project_dir)
sys.path.append(dags_dir)
from test_gx.custom_expectations.expect_column_values_to_meet_date_condition import ExpectColumnValuesToMeetDateCondition
class ExpectColumnValuesToMeetDateConditionUuid(ExpectColumnValuesToMeetDateCondition):
    domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser"
    ) + ("uuid",) # 新增uuid作为domain_key

from great_expectations.expectations.core.expect_column_values_to_not_be_null import ExpectColumnValuesToNotBeNull
class ExpectColumnValuesToNotBeNullUuid(ExpectColumnValuesToNotBeNull):
    domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    ) + ("uuid",)

test_data_dir = os.path.join(project_dir, "test_data")
validation_result_dir = os.path.join(project_dir, "validation_result")
spark = SparkSession.builder.appName("gx_test").getOrCreate()

# 读取数据
spark_df = spark.read.csv(os.path.join(test_data_dir, "test_data_02.csv"), header=True, inferSchema=True)
pandas_df = pd.read_csv(os.path.join(test_data_dir, "test_data_02.csv"))
context = gx.get_context()
test_id = os.path.basename(__file__).replace(".py", "")

# ===== Spark 测试 =====
print("=== Testing Spark Engine ===")
spark_source_name = f"{test_id}_spark_data_source"
spark_data_source: Datasource = context.sources.add_or_update_spark(name=spark_source_name)
spark_data_asset: DataAsset = spark_data_source.add_dataframe_asset(name=f"{test_id}_spark_asset", dataframe=spark_df)
spark_batch_request: BatchRequest = spark_data_asset.build_batch_request()
spark_suite_name = f"{test_id}_spark_suite"
context.add_or_update_expectation_suite(expectation_suite_name=spark_suite_name)
spark_validator: Validator = context.get_validator(batch_request=spark_batch_request, expectation_suite_name=spark_suite_name)

spark_validator.expect_column_values_to_meet_date_condition_uuid(column="order_date", uuid="00001")
spark_validator.expect_column_values_to_not_be_null_uuid(column="order_date", uuid="00001")
spark_validator.expect_column_values_to_meet_date_condition_uuid(column="order_date", uuid="00002")
spark_validator.expect_column_values_to_not_be_null_uuid(column="order_date", uuid="00002")

# ===== pandas 测试 =====
print("=== Testing Pandas Engine ===")
pandas_source_name = f"{test_id}_pandas_data_source"
pandas_data_source: Datasource = context.sources.add_or_update_pandas(name=pandas_source_name)
pandas_data_asset: DataAsset = pandas_data_source.add_dataframe_asset(name=f"{test_id}_pandas_asset", dataframe=pandas_df)
pandas_batch_request: BatchRequest = pandas_data_asset.build_batch_request()
pandas_suite_name = f"{test_id}_pandas_suite"
context.add_or_update_expectation_suite(expectation_suite_name=pandas_suite_name)
pandas_validator: Validator = context.get_validator(batch_request=pandas_batch_request, expectation_suite_name=pandas_suite_name)

pandas_validator.expect_column_values_to_meet_date_condition_uuid(column="order_date", uuid="00001")
pandas_validator.expect_column_values_to_not_be_null_uuid(column="order_date", uuid="00001")
pandas_validator.expect_column_values_to_meet_date_condition_uuid(column="order_date", uuid="00002")
pandas_validator.expect_column_values_to_not_be_null_uuid(column="order_date", uuid="00002")

# ===== 验证结果 =====
result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["index"],
    "return_unexpected_index_query": False,
}

print("=== Spark Results ===")
spark_results = spark_validator.validate(result_format=result_format)
with open(os.path.join(validation_result_dir, f"{test_id.split('_')[0]}_result_{test_id.split('_')[1]}_spark.json"), "w", encoding="utf-8") as f:
    f.write(str(spark_results))

print("=== Pandas Results ===")
pandas_results = pandas_validator.validate(result_format=result_format)
with open(os.path.join(validation_result_dir, f"{test_id.split('_')[0]}_result_{test_id.split('_')[1]}_pandas.json"), "w", encoding="utf-8") as f:
    f.write(str(pandas_results))

print("Testing completed!")
