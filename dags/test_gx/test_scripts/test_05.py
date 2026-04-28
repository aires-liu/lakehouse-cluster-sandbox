import os
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
from pyspark.sql import SparkSession
import sys

# 通过继承Expectation类并改写_validate方法来实现自定义的期望
# 支持对pandas和Spark DataFrame的验证
# 这里是对Spark的验证
current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
dags_dir = os.path.dirname(project_dir)
sys.path.append(dags_dir)
from test_gx import custom_expectations
test_data_dir = os.path.join(project_dir, "test_data")
validation_result_dir = os.path.join(project_dir, "validation_result")
spark = SparkSession.builder.appName("gx_test").getOrCreate()

# 1. 读取测试数据
test_df = spark.read.csv(os.path.join(test_data_dir, "test_data_02.csv"), header=True, inferSchema=True)
print(test_df.show())

# 2. 获取GX context
context = gx.get_context()

# 3. 注册Spark数据源
source_name = "test_05_spark_data_source"
data_source: Datasource = context.sources.add_or_update_spark(name=source_name)

# 4、创建数据资产
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_05_asset", dataframe=test_df)

# 5. 创建批处理请求
batch_request: BatchRequest = data_asset.build_batch_request()

# 6. 创建期望规则集
suite_name = "test_05_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 7. 获取验证器
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 8. 添加期望规则
validator.expect_column_values_to_meet_date_condition_v1(column="order_date", date='2024-09-08', operator=">=")
validator.expect_column_values_to_be_in_set("payment_method", ["credit_card", "paypal", "bank_transfer", "cash"])

# 10、统一设置输出格式（实际上这种方式自定义的期望无法直接响应GX的输出格式）
result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["order_id"],
    "return_unexpected_index_query": True,
}

# 10. 执行校验
results = validator.validate(result_format=result_format)

# 9. 输出校验结果
with open(os.path.join(validation_result_dir, "test_result_05.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

