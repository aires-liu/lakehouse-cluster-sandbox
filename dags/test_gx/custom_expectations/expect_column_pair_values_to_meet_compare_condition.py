import pandas as pd
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import ColumnPairMapMetricProvider, column_pair_condition_partial
from pyspark.sql.column import Column as SparkColumn
import operator as op # op的运算符和比较符可以直接用于Series和Column对象

class ColumnPairValuesCompareCondition(ColumnPairMapMetricProvider):
    condition_metric_name = "column_values.pair_values_compare_condition"
    condition_value_keys = ("operator",)

    @staticmethod
    def _get_operator(col_A, col_B, operator):
        ops = {
            ">=": op.ge,
            "<=": op.le,
            ">": op.gt,
            "<": op.lt,
            "==": op.eq
        }
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator}. Supported operators are: {', '.join(ops.keys())}")
        return ops[operator](col_A, col_B)

    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A: pd.Series, column_B: pd.Series, **kwargs):
        operator = kwargs["operator"]
        col_A = pd.to_numeric(column_A, errors="coerce")
        col_B = pd.to_numeric(column_B, errors="coerce")
        mask = col_A.notna() & col_B.notna() & cls._get_operator(col_A, col_B, operator)
        return mask

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A: SparkColumn, column_B: SparkColumn, **kwargs):
        operator = kwargs["operator"]
        col_A = column_A.cast("double")
        col_B = column_B.cast("double")
        mask = col_A.isNotNull() & col_B.isNotNull() & cls._get_operator(col_A, col_B, operator)
        return mask

class ExpectColumnPairValuesToMeetCompareCondition(ColumnPairMapExpectation):
    """
    比较两列数值大小关系, 不可转换为数值的字符串、空白符等返回False

    - 默认比较符为"==", 支持">=", "<=", ">", "<", "=="

    示例:
        expect_column_pair_values_to_meet_compare_condition(column_A="list_price", column_B="sale_price", operator=">=")
        验证list_price的值是否大于等于sale_price的值
        expect_column_pair_values_to_meet_compare_condition(column_A="list_price", column_B="sale_price")
        验证list_price的值是否等于sale_price的值
    """
    map_metric = "column_values.pair_values_compare_condition"
    success_keys = ("mostly", "column_A", "column_B", "operator", "ignore_row_if")
    default_kwarg_values = {
        "mostly": 1.0,
        "operator": "==",
        "ignore_row_if": "either_value_is_missing"
    }