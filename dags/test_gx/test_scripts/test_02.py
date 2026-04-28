import os
import pandas as pd
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator

current_dir = os.path.dirname(os.path.realpath(__file__))
test_data_dir = os.path.join(os.path.dirname(current_dir), "test_data")
validation_result_dir = os.path.join(os.path.dirname(current_dir), "validation_result")

# 1. 读取测试数据
test_df = pd.read_csv(os.path.join(test_data_dir, "test_data_01.csv"))
print(test_df.to_markdown())

# 2. 获取GX context
context = gx.get_context()

# 3. 注册pandas数据源
# context.sources是一个数据源管理器，存储所有已注册的数据源，支持字典式的访问但本身不可迭代
source_name = "test_02_pandas_data_source"
data_source: Datasource = context.sources.add_or_update_pandas(name=source_name)

# 4、创建数据资产
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_02_asset", dataframe=test_df)

# 5. 创建批处理请求
batch_request: BatchRequest = data_asset.build_batch_request()

# 6. 创建期望规则集
suite_name = "test_02_suite"
suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 7. 获取验证器
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 8. 添加期望规则
validator.expect_column_values_to_not_be_null("price")
validator.expect_column_values_to_not_be_null("payment_method")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_be_unique("status")
validator.expect_column_values_to_be_between("price", min_value=200)
validator.expect_column_values_to_be_between("discount", min_value=0.2)
validator.expect_column_mean_to_be_between("product_id", min_value=10, max_value=200)
validator.expect_column_mean_to_be_between("price", max_value=20)
validator.expect_column_values_to_be_in_set("status", ["paid", "pending", "cancelled", "refunded"])
validator.expect_column_values_to_be_in_set("payment_method", ["credit_card", "paypal", "bank_transfer", "cash"])
validator.expect_column_values_to_not_be_in_set("city", ["Passed", "Failed"])
validator.expect_column_values_to_not_be_in_set("status", ["paid", "pending", "cancelled", "refunded"])

# 9. 保存suite（可选），discard_failed_expectations=False 确保期望失败也保存suite到GX项目目录中
# 如果不使用save_expectation_suite，suite将不会持久化，临时保存在内存中，无法复用和管理
# 持久化suite可以在下次校验同类数据时，无需重新编写规则，直接加载suite即可
# suite文件（通常是 JSON）可以纳入git等版本控制，方便团队协作、回溯和审计
# 在CI/CD、定时任务、数据管道等场景下，可自动加载suite进行批量数据验证
# GX支持生成数据文档（Data Docs），持久化的suite可自动生成可视化校验报告
# 所有期望规则集中存放，便于维护、查找和统一修改
context.save_expectation_suite(expectation_suite=suite, discard_failed_expectations=False)

# 10、统一设置输出格式
result_format = {
    "result_format": "COMPLETE", # 输出结果的详细程度，包括BASIC、SUMMARY、COMPLETE
    "unexpected_index_column_names": ["order_id"], # 返回非期望值的同时返回其对应的order_id列的值
    "return_unexpected_index_query": True, # 返回非期望值的查询语句
}

# 10. 执行校验
results = validator.validate(result_format=result_format)

# 9. 输出校验结果
with open(os.path.join(validation_result_dir, "test_result_02.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

