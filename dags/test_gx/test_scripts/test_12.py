import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
import sys
import pandas as pd
from pyspark.sql import SparkSession

# 继承自高层expectation的期望都会自动过滤空值的校验
# 这里讨论如何规避result中的unexpected_list和unexpected_index_list过滤空值的问题
# 注意result中的unexpected_count是独立的计算逻辑，这里使用方法不会影响unexpected_count的过滤空值的计算结果
# 因此可能会出现unexpected_count与unexpected_list/unexpected_index_list的元素数量不一致的情况
# 需要明确，对单列的期望校验，传入的domain数据是没有过滤空值的，返回的布尔数组是完整的，过滤空值的行为在构造result的时候进行
# 对于双列及多列的期望校验，传入的domain数据是可以过滤空值并可以通过期望入参控制，所以校验逻辑中是可能接收不到空值的行数据的
# 因此对于双列及多列的期望校验中Metric返回的布尔数组在构造result不会进行任何过滤行为
# 以下分别提供单列、双列、多列期望的规避输出result中unexpected_list和unexpected_index_list空值过滤的方法
# 分自定义期望和内置期望两种情况测试，以下测试均已通过（great_expectation==0.18.21）

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
dags_dir = os.path.dirname(project_dir)
sys.path.append(dags_dir)
'''
1、测试继承自ColumnMapExpectation的自定义期望
继承ColumnMapExpectation的期望传入的domain是不进行空值过滤的，在输出unexpected_index_list时进行空值过滤
核心逻辑参考map_condition_auxilliary_methods.py
'''
from test_gx.custom_expectations.expect_column_values_to_meet_date_condition import (
    ExpectColumnValuesToMeetDateCondition,
    ColumnValuesMeetDateCondition
)
class ColumnValuesMeetDateConditionNfn(ColumnValuesMeetDateCondition):
    condition_metric_name = "column_values.meet_date_condition_not_filter_null"
    filter_column_isnull = False
class ExpectColumnValuesToMeetDateConditionNfn(ExpectColumnValuesToMeetDateCondition):
    map_metric = "column_values.meet_date_condition_not_filter_null"
'''
2、测试继承自ColumnPairMapExpectation的自定义期望
继承ColumnPairMapExpectation的期望通过ignore_row_if参数控制传入的domain是否进行空值过滤
也可以不修改原期望默认参数值，在调用此类期望时通过传入ignore_row_if参数控制
"both_values_are_missing" 或 "either_value_is_missing" 或 "neither"
在输出unexpected_index_list时不会进行任何处理，这点与继承自ColumnMapExpectation的期望不同
核心逻辑参考map_condition_auxilliary_methods.py及pandas_execution_engine.py
这里通过修改原自定义期望的默认参数值来规避空值过滤
注意：对任意一列为空值的行的处理逻辑及返回结果需要在metric中实现
'''
from test_gx.custom_expectations.expect_column_pair_values_to_meet_compare_condition import ExpectColumnPairValuesToMeetCompareCondition
class ExpectColumnPairValuesToMeetCompareConditionNfn(ExpectColumnPairValuesToMeetCompareCondition):
    default_kwarg_values = {
        "mostly": 1.0,
        "operator": "==",
        "ignore_row_if": "neither"
    } 
'''
3、测试继承自MulticolumnMapExpectation的自定义期望
与2类似也是通过ignore_row_if参数控制传入的domain是否进行空值过滤，但是参数值不太一样
"all_values_are_missing" 或 "any_value_is_missing" 或 "never"
这里通过修改原自定义期望的默认参数值来规避空值过滤
'''
from test_gx.custom_expectations.expect_columns_arithmetic_to_equals_result_column import ExpectColumnsArithmeticToEqualsResultColumn
class ExpectColumnsArithmeticToEqualsResultColumnNfn(ExpectColumnsArithmeticToEqualsResultColumn):
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "never",
        "tolerance": 1e-2,
    }
'''
4、测试继承自ColumnMapExpectation的内置期望
与1类似，通过重写metric的属性并重新指定期望使用的metric即可规避空值过滤
'''
from great_expectations.expectations.core import ExpectColumnValuesToBeBetween
from great_expectations.expectations.metrics.column_map_metrics import ColumnValuesBetween
class ColumnValuesBetweenNfn(ColumnValuesBetween):
    condition_metric_name = "column_values.between_not_filter_null"
    filter_column_isnull = False
class ExpectColumnValuesToBeBetweenNfn(ExpectColumnValuesToBeBetween):
    map_metric = "column_values.between_not_filter_null"
'''
5、测试继承自MulticolumnMapExpectation的内置期望
测试的内置期望expect_column_pair_values_to_be_equal默认ignore_row_if参数值为both_values_are_missing
这里通过调用时传入ignore_row_if参数值为neither来规避空值过滤
'''
# expect_column_pair_values_to_be_equal(column_A="price", column_B="product_id", ignore_row_if="neither")
'''
6、测试继承自ColumnPairMapExpectation的内置期望
测试的内置期望expect_multicolumn_sum_to_equal默认ignore_row_if参数值为all_values_are_missing
这里通过调用时传入ignore_row_if参数值为neither来规避空值过滤
'''
# expect_multicolumn_sum_to_equal(column_list=["order_id", "user_id", "product_id"], value=100, ignore_row_if="never")

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

# spark_validator.expect_column_values_to_meet_date_condition_nfn(column="order_date", date="2023-01-15", operator=">=")
spark_validator.expect_column_values_to_be_between_nfn(column="price", min_value=50, max_value=500)
# spark_validator.expect_column_pair_values_to_meet_compare_condition_nfn(column_A="price", column_B="product_id", operator=">=")
# spark_validator.expect_column_pair_values_to_be_equal(column_A="price", column_B="product_id", ignore_row_if="neither")
# spark_validator.expect_columns_arithmetic_to_equals_result_column_nfn(expr="{0}*{1}", column_list=["price", "discount", "cost"])
# spark_validator.expect_multicolumn_sum_to_equal(column_list=["order_id", "user_id", "product_id"], value=100, ignore_row_if="never")

# ===== pandas 测试 =====
print("=== Testing Pandas Engine ===")
pandas_source_name = f"{test_id}_pandas_data_source"
pandas_data_source: Datasource = context.sources.add_or_update_pandas(name=pandas_source_name)
pandas_data_asset: DataAsset = pandas_data_source.add_dataframe_asset(name=f"{test_id}_pandas_asset", dataframe=pandas_df)
pandas_batch_request: BatchRequest = pandas_data_asset.build_batch_request()
pandas_suite_name = f"{test_id}_pandas_suite"
context.add_or_update_expectation_suite(expectation_suite_name=pandas_suite_name)
pandas_validator: Validator = context.get_validator(batch_request=pandas_batch_request, expectation_suite_name=pandas_suite_name)

# pandas_validator.expect_column_values_to_meet_date_condition_nfn(column="order_date", date="2023-01-15", operator=">=")
pandas_validator.expect_column_values_to_be_between_nfn(column="price", min_value=50, max_value=500)
# pandas_validator.expect_column_pair_values_to_meet_compare_condition_nfn(column_A="price", column_B="product_id", operator=">=")
# pandas_validator.expect_column_pair_values_to_be_equal(column_A="price", column_B="product_id", ignore_row_if="neither")
# pandas_validator.expect_columns_arithmetic_to_equals_result_column_nfn(expr="{0}*{1}", column_list=["price", "discount", "cost"])
# pandas_validator.expect_multicolumn_sum_to_equal(column_list=["order_id", "user_id", "product_id"], value=100, ignore_row_if="never")

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
