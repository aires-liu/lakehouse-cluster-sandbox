import great_expectations as gx
import pandas as pd
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
test_data_dir = os.path.join(os.path.dirname(current_dir), "test_data")
validation_result_dir = os.path.join(os.path.dirname(current_dir), "validation_result")
# 1. 读取测试数据
df = pd.read_csv(os.path.join(test_data_dir, "test_data_01.csv"))
# GX支持pandas DataFrame、spark DataFrame、SQL数据库表、CSV、Parquet文件或自定义Batch作为数据源
print(df.to_markdown())

# 2. 用 gx.from_pandas(df) 创建 GX DataFrame
gx_df = gx.from_pandas(df)
# 使用gx.from_pandas()方法将pandas DataFrame转换为GX DataFrame对象
# 转换后的GX DataFrame对象可以使用GX的校验方法, 相当于升级了原本的普通DataFrame

# 3. 添加期望规则
# 这里的作用是为gx_df添加校验规则, 但不会立即对数据进行校验

# 3.1 列存在性与列类型
gx_df.expect_column_to_exist("order_id", result_format="COMPLETE")
gx_df.expect_column_to_exist("pay_date", result_format="COMPLETE")
gx_df.expect_column_values_to_be_of_type("order_id", "int64", result_format="COMPLETE")
gx_df.expect_column_values_to_be_of_type("order_date", "int64", result_format="COMPLETE")

# 3.2 空值与唯一性
gx_df.expect_column_values_to_not_be_null("price", result_format="COMPLETE")
gx_df.expect_column_values_to_not_be_null("payment_method", result_format="COMPLETE")
gx_df.expect_column_values_to_be_unique("order_id", result_format="COMPLETE")
gx_df.expect_column_values_to_be_unique("status", result_format="COMPLETE")

# 3.3 数据范围
# 在0.18.2版本的GX中对同一列验证同一条规则，即使参数不一致输出的json中仍然只有一条结果
gx_df.expect_column_values_to_be_between("product_id", min_value=100, max_value=200)
# 每个期望规则都有result_format参数可以指定验证结果的输出格式，用于控制result的详细程度，默认是BASIC
gx_df.expect_column_values_to_be_between("price", min_value=200, result_format="SUMMARY")
gx_df.expect_column_values_to_be_between("discount", min_value=0.2, result_format="COMPLETE")
gx_df.expect_column_mean_to_be_between("product_id", min_value=10, max_value=200)
gx_df.expect_column_mean_to_be_between("price", max_value=20)

# 3.4 集合与成员
gx_df.expect_column_values_to_be_in_set("status", ["paid", "pending", "cancelled", "refunded"])
gx_df.expect_column_values_to_be_in_set("payment_method", ["credit_card", "paypal", "bank_transfer", "cash"])
gx_df.expect_column_values_to_not_be_in_set("city", ["Passed", "Failed"])
gx_df.expect_column_values_to_not_be_in_set("status", ["paid", "pending", "cancelled", "refunded"])

# 3.5 字符串相关
gx_df.expect_column_values_to_match_strftime_format("order_date", "%Y-%m-%d")
gx_df.expect_column_values_to_match_regex("payment_method", r"^(.*_.*)$")

# 3.6 表级别
# 该规则对同一张表的多次验证也只会输出一条结果
gx_df.expect_table_row_count_to_be_between(25, 45)
gx_df.expect_table_column_count_to_be_between(5, 8)

# 3.7 日期与时间
gx_df.expect_column_values_to_be_dateutil_parseable("order_date")

# 3.8 规律
gx_df.expect_column_values_to_be_increasing("discount")

# 4. 执行验证
results = gx_df.validate()
# 通过调用validate()方法来执行上面添加的校验规则的验证并返回校验结果
# 校验结果是一个类似json的字典对象, 包含了每个期望规则的验证结果
# 结果中会包含每个期望规则的验证状态、失败的行数、异常值等信息

# 5. 输出结果
with open(os.path.join(validation_result_dir, "test_result_01.json"), "w", encoding="utf-8") as f:
    f.write(str(results))
# 这种校验方式属于GX的简单校验，输出日志中不会有Calculating Metrics等信息