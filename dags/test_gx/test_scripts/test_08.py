import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
import sys
from pyspark.sql import SparkSession
    
# GX的内置期望和基于ColumnMapMetricProvider自定义的期望都会跳过空值，即结果json中的missing_count
# 但是实际数据校验中我们常常期待输出的失败结果是包括空值及其对应索引行的
# 这是GX验证中非常底层的逻辑，目前没有什么好的方法能避免跳过
# 如果想输出的结果中包含空值的行，可以通过结合expect_column_values_to_not_be_null期望验证
# 从GX提取结果时通过meta来统一取出来
current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
dags_dir = os.path.dirname(project_dir)
sys.path.append(dags_dir)
from test_gx import custom_expectations
from test_gx.utils.parse_gx_result import ParsedGXResult
test_data_dir = os.path.join(project_dir, "test_data")
validation_result_dir = os.path.join(project_dir, "validation_result")
spark = SparkSession.builder.appName("gx_test").getOrCreate()

test_df = spark.read.csv(os.path.join(test_data_dir, "test_data_02.csv"), header=True, inferSchema=True)
print(test_df.show())
context = gx.get_context()
source_name = "test_08_spark_data_source"
data_source: Datasource = context.sources.add_or_update_spark(name=source_name)
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_08_asset", dataframe=test_df)
batch_request: BatchRequest = data_asset.build_batch_request()
suite_name = "test_08_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 这里对每个字段的校验都用两个期望来实现，最后通过meta来合并输出结果
validator.expect_column_values_to_meet_date_condition(column="order_date", date="2024-09-07", operator=">=", meta={"Rule": "R0001"})
validator.expect_column_values_to_not_be_null(column="order_date", meta={"Rule": "R0001"})

validator.expect_column_values_to_match_date_format(column="order_date", date_format="YYYY-MM-DD", meta={"Rule": "R0002"})
validator.expect_column_values_to_not_be_null(column="order_date", meta={"Rule": "R0002"})

validator.expect_column_values_to_be_between("discount", min_value=0.2, meta={"Rule": "R0003"})
validator.expect_column_values_to_not_be_null(column="discount", meta={"Rule": "R0003"})

validator.expect_column_values_to_be_in_set("status", ["paid", "pending", "cancelled", "refunded"], meta={"Rule": "R0004"})
validator.expect_column_values_to_not_be_null(column="status", meta={"Rule": "R0004"})

result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["index"],
    "return_unexpected_index_query": True,
}


results = validator.validate(result_format=result_format)
with open(os.path.join(validation_result_dir, "test_result_08.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

result_df = ParsedGXResult(results).get_dataframe()
print(result_df.fillna("nan").to_markdown())

# 对于object类型中的空值，to_markdown()渲染时会输出为空字符串所以这里做个填充替换

