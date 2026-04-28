import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
from pyspark.sql import SparkSession
import sys
    
# 前面通过row_condition这个参数来解决了期望覆盖的问题
# 实际上row_condition可以实现不同期望验证同一列的不同部分来达成比较复杂的验证规则

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
sys.path.append(project_dir)
from utils.parse_gx_result import ParsedGXResult
test_data_dir = os.path.join(project_dir, "test_data")
validation_result_dir = os.path.join(project_dir, "validation_result")
spark = SparkSession.builder.appName("gx_test").getOrCreate()

test_df = spark.read.csv(os.path.join(test_data_dir, "test_data_02.csv"), header=True, inferSchema=True)
print(test_df.show())
context = gx.get_context()
source_name = "test_10_spark_data_source"
data_source: Datasource = context.sources.add_or_update_spark(name=source_name)
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_10_asset", dataframe=test_df)
batch_request: BatchRequest = data_asset.build_batch_request()
suite_name = "test_10_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# R0001: 验证payment_method列为cash和credit_card类型时price是否大于100
validator.expect_column_values_to_be_between(column="price", \
                                             min_value=100, \
                                             row_condition='payment_method IN ("cash", "credit_card")', \
                                             condition_parser="spark", \
                                             meta={"Rule": "R0001"})
                                             # SQL表达式必须使用双引号，单引号会导致GX无法序列化
validator.expect_column_values_to_not_be_null(column="price", \
                                              meta={"Rule": "R0001"}, \
                                              row_condition='payment_method IN ("cash", "credit_card")', \
                                              condition_parser="spark")

result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["index"],
    "return_unexpected_index_query": True,
}

results = validator.validate(result_format=result_format)
with open(os.path.join(validation_result_dir, "test_result_10.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

result_df = ParsedGXResult(results).get_dataframe()
print(result_df.fillna("nan").to_markdown())