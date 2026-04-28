import pandas as pd
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import ColumnPairMapMetricProvider, column_pair_condition_partial
import pyspark.sql.functions as F
from pyspark.sql.column import Column as SparkColumn
import operator as op 

class ColumnPairDaysDiffMeetCondition(ColumnPairMapMetricProvider):
    condition_metric_name = "column_values.days_diff_meet_condition"
    condition_value_keys = ("operator", "days")

    @staticmethod
    def _get_operator(diff, days, operator):
        ops = {
            ">=": op.ge,
            "<=": op.le,
            ">": op.gt,
            "<": op.lt,
            "==": op.eq
        }
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator}. Supported operators are: {', '.join(ops.keys())}")
        return ops[operator](diff, days)

    # ColumnPairMapMetricProvider的方法签名固定是Column_A和Column_B
    # 这里不同的是自定义参数operator和days必须从kwargs中获取，不能显式传递
    @column_pair_condition_partial(engine=PandasExecutionEngine) # **kwargs不能删除
    def _pandas(cls, column_A: pd.Series, column_B: pd.Series, **kwargs):
        operator = kwargs["operator"]
        days = kwargs["days"]
        date_A = pd.to_datetime(column_A, format="%Y-%m-%d", errors="coerce")
        date_B = pd.to_datetime(column_B, format="%Y-%m-%d", errors="coerce")
        diff = (date_A - date_B).dt.days
        mask = date_A.notna() & date_B.notna() & cls._get_operator(diff, days, operator)
        return mask

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A: SparkColumn, column_B: SparkColumn, **kwargs):
        operator = kwargs["operator"]
        days = kwargs["days"]
        date_A = F.to_date(column_A, "yyyy-MM-dd")
        date_B = F.to_date(column_B, "yyyy-MM-dd")
        diff = F.datediff(date_A, date_B)
        mask = date_A.isNotNull() & date_B.isNotNull() & cls._get_operator(diff, F.lit(days), operator)
        return mask
    # spark dataframe继承多列校验时，返回的日期值会在验证结果中序列化为"YYYY-MM-DDTHH:MM:SS" 这种 ISO 格式
    # 这是GX框架的默认行为，属于兼容和通用性设计，对结果进行处理时需要注意这一点
    # 如果原始数据中有其他非日期格式的数据就会返回原始值（奇怪的特性）

class ExpectColumnPairDaysDiffToMeetCondition(ColumnPairMapExpectation):
    """
    验证两列日期的差值是否满足指定的条件

    - 日期格式为YYYY-MM-DD
    - 默认比较符为"==", 支持">=", "<=", ">", "<", "=="
    - 支持验证pandas和Spark DataFrame
    - 验证column_A - column_B的天数差，支持0和负数天数差

    示例:
        expect_column_pair_days_diff_to_meet_condition(column_A="product_date", column_B="order_date", operator=">=", days=0)
        验证product_date的日期是否大于等于order_date的日期
        expect_column_pair_days_diff_to_meet_condition(column_A="product_date", column_B="order_date")
        验证product_date的日期是否等于order_date的日期
        expect_column_pair_days_diff_to_meet_condition(column_A="product_date", column_B="order_date", operator=">", days=-3)
        验证product_date的日期是否大于order_date的3天前的日期
    """
    map_metric = "column_values.days_diff_meet_condition"
    success_keys = ("mostly", "column_A", "column_B", "operator", "days", "ignore_row_if") 
    # 声明哪些参数影响验证逻辑和结果所以也需要 ignore_row_if 否则会参数丢失变成 None
    default_kwarg_values = {
        "mostly": 1.0,
        "operator": "==",
        "days": 0,
        "ignore_row_if": "either_value_is_missing" # 多列验证时原本默认的ignore_row_if=None会报错
    }
    # both_values_are_missing 只有当两列的值都缺失（为空或NaN）时才跳过该行
    # either_value_is_missing 只要任意一列的值缺失（为空或NaN）就跳过该行
    # neither 不跳过任何行，即使有缺失值也参与验证
