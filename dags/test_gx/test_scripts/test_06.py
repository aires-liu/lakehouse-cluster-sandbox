import os
import pandas as pd
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
import sys

# 这是一个通过继承ColumnMapExpectation来实现原本的expect_column_values_to_meet_date_condition期望
# 相比较于V1，代码量更少且可以直接使用GX的内置验证逻辑，而无需手动处理验证结果
# 这里是对pandas的验证
# 补充：增加对自定义expect_column_values_to_match_date_format期望的校验
current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(current_dir)
dags_dir = os.path.dirname(project_dir)
sys.path.append(dags_dir)
from test_gx import custom_expectations
test_data_dir = os.path.join(project_dir, "test_data")
validation_result_dir = os.path.join(project_dir, "validation_result")
# 1. 读取测试数据
test_df = pd.read_csv(os.path.join(test_data_dir, "test_data_02.csv"))
print(test_df.to_markdown())

# 2. 获取GX context
context = gx.get_context()

# 3. 注册pandas数据源
source_name = "test_06_pandas_data_source"
data_source: Datasource = context.sources.add_or_update_pandas(name=source_name)

# 4、创建数据资产
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_06_asset", dataframe=test_df)

# 5. 创建批处理请求
batch_request: BatchRequest = data_asset.build_batch_request()

# 6. 创建期望规则集
suite_name = "test_06_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 7. 获取验证器
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 8. 添加期望规则
validator.expect_column_values_to_meet_date_condition(column="order_date", date="2024-09-07", operator=">=")
validator.expect_column_values_to_match_date_format(column="order_date", date_format="YYYY-MM-DD")
validator.expect_column_values_to_be_between("discount", min_value=0.2)

# 10、统一设置输出格式
result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["index"],
    "return_unexpected_index_query": True,
}

# 10. 执行校验
results = validator.validate(result_format=result_format)

# 9. 输出校验结果
with open(os.path.join(validation_result_dir, "test_result_06.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

