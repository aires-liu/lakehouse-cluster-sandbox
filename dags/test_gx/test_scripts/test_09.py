import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
import sys
from pyspark.sql import SparkSession
    
# 前面已经尝试通过期望组合来规避跳过空值的问题，但是又产生了新的问题
# 如果有两条验证规则都校验同一个字段，并且都用了组合期望的方式来解决跳过空值的问题
# 本质上是expect_column_values_to_not_be_null对一个字段验证了两次，后者会覆盖前者
# 此时我们就可以考虑通过row_condition这个参数来解决这个问题

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
source_name = "test_09_spark_data_source"
data_source: Datasource = context.sources.add_or_update_spark(name=source_name)
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_09_asset", dataframe=test_df)
batch_request: BatchRequest = data_asset.build_batch_request()
suite_name = "test_09_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 这里一共有两条验证规则，通过row_condition参数来实现输出四个result避免覆盖问题
# row_condition参数的值可以是任意SQL表达式，GX会将其转换为Spark SQL的表达式
# condition_parser参数可以指定解析row_condition的方式，pandas dataframe的解析器是pandas
validator.expect_column_values_to_meet_date_condition(column="order_date", date="2024-09-07", operator=">=", meta={"Rule": "R0001"})
validator.expect_column_values_to_not_be_null(column="order_date", meta={"Rule": "R0001"}, row_condition="1=1", condition_parser="spark")

validator.expect_column_values_to_match_date_format(column="order_date", date_format="YYYY-MM-DD", meta={"Rule": "R0002"})
validator.expect_column_values_to_not_be_null(column="order_date", meta={"Rule": "R0002"}, row_condition="2=2", condition_parser="spark")

result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["index"],
    "return_unexpected_index_query": True,
}

results = validator.validate(result_format=result_format)
with open(os.path.join(validation_result_dir, "test_result_09.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

result_df = ParsedGXResult(results).get_dataframe()
print(result_df.fillna("nan").to_markdown())
# 对于object类型中的空值，to_markdown()渲染时会输出为空字符串所以这里做个填充替换
