import os
import pandas as pd
import great_expectations as gx
from great_expectations.datasource.fluent import Datasource, DataAsset, BatchRequest
from great_expectations.validator.validator import Validator
from great_expectations.expectations.expectation import Expectation

current_dir = os.path.dirname(os.path.realpath(__file__))
test_data_dir = os.path.join(os.path.dirname(current_dir), "test_data")
validation_result_dir = os.path.join(os.path.dirname(current_dir), "validation_result")

# 自定义期望名按GX规则自动映射，即类名去掉Expect前缀、驼峰转下划线
class ExpectColumnValuesToBeGreaterThanOneHundred(Expectation): # 自定义期望继承Expectation类
    """验证列值大于100"""
    metric_dependencies = () # 不注册任何GX预定义的依赖，直接重写_validate方法来实现
    success_keys = ("column", "mostly") # 设置该期望通过什么参数来判断是否通过

    # 期望的核心验证逻辑，_balidate是GX在执行验证时调用的内部方法
    def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
        # cinfiguration包含用户传入的参数，比如验证的字段，验证的通过比例等
        # execution_engine是GX当前的执行引擎，可以获取到DataFrame
        column = configuration.kwargs["column"] # 获取要验证的列名
        mostly = configuration.kwargs.get("mostly", 1.0) # 获取通过比例
        df = execution_engine.dataframe

        # 获取布尔值列表
        condition_mask = df[column] > 100
        # 计算比率
        positive_ratio = condition_mask.mean()
        success = positive_ratio >= mostly
        # 获取不满足条件的值和索引
        unexpected_mask = ~condition_mask
        unexpected_values = df.loc[unexpected_mask, column].tolist()
        unexpected_indexes = df.index[unexpected_mask].tolist()

        # 这里的语法只支持pandas
        return {
            "success": success,
            "result": {
                "positive_ratio": positive_ratio,
                "unexpected_list": unexpected_values,
                "unexpected_index_list": unexpected_indexes,
                "element_count": len(df),
                "unexpected_count": len(unexpected_values),
                "unexpected_percent": round(len(unexpected_values) / len(df), 4),
            }
        } # 这里返回的内容就是GX验证结果中result的内容
    # 这种重写_validate方法自定义期望的方式不会受到result_format参数的影响，需要手动构建所有的输出结果
    
# 1. 读取测试数据
test_df = pd.read_csv(os.path.join(test_data_dir, "test_data_01.csv"))
print(test_df.to_markdown())

# 2. 获取GX context
context = gx.get_context()

# 3. 注册pandas数据源
source_name = "test_03_pandas_data_source"
data_source: Datasource = context.sources.add_or_update_pandas(name=source_name)

# 4、创建数据资产
data_asset: DataAsset = data_source.add_dataframe_asset(name="test_03_asset", dataframe=test_df)

# 5. 创建批处理请求
batch_request: BatchRequest = data_asset.build_batch_request()

# 6. 创建期望规则集
suite_name = "test_03_suite"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# 7. 获取验证器
validator: Validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

# 8. 添加期望规则
validator.expect_column_values_to_be_greater_than_one_hundred(column="price")
validator.expect_column_values_to_not_be_in_set("status", ["paid", "pending", "cancelled", "refunded"])

# 10、统一设置输出格式
result_format = {
    "result_format": "COMPLETE", 
    "unexpected_index_column_names": ["order_id"],
    "return_unexpected_index_query": True,
}

# 10. 执行校验
results = validator.validate(result_format=result_format)

# 9. 输出校验结果
with open(os.path.join(validation_result_dir, "test_result_03.json"), "w", encoding="utf-8") as f:
    f.write(str(results))

