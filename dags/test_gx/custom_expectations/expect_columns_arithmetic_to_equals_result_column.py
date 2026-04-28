import pandas as pd
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import MulticolumnMapMetricProvider, multicolumn_condition_partial
import pyspark.sql.functions as F
from pyspark.sql.column import Column as SparkColumn
from typing import Union, List
from functools import reduce
from operator import and_

class ColumnArithmeticEqualsResult(MulticolumnMapMetricProvider):
    condition_metric_name = "column_arithmetic_equals_result_column"
    condition_value_keys = ("expr", "column_list", "tolerance")

    @staticmethod
    def _apply_expr(
        operand_cols: List[Union[pd.Series, SparkColumn]], 
        expr: str
    ) -> Union[pd.Series, SparkColumn]:
        # format方法用于将字符串中的占位符替换为指定的值
        # expr = "({0}+{1}-{2})*{3}" lst = ["a", "b", "c", "d"]
        # expr.format(*lst) 输出: (a+b-c)*d
        expr_for_eval = expr.format(*[f"operand_cols[{i}]" for i in range(len(operand_cols))])
        # eval方法用于动态执行字符串表达式并返回结果
        # result = eval(expression, globals=None, locals=None)
        # expression：要计算的字符串表达式，globals（可选）：用于指定全局命名空间，locals（可选）：用于指定局部命名空间
        # 通过{"__builtins__": None}来禁用eval执行环境中的所有Python内置函数和变量，只能访问显式传入的变量避免执行恶意代码
        result = eval(expr_for_eval,  {"__builtins__": None}, {"operand_cols": operand_cols})
        return result

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, df: pd.DataFrame, **kwargs):
        expr, tolerance, column_list = kwargs["expr"], kwargs["tolerance"], kwargs["column_list"]
        *operand_cols, result_column = [pd.to_numeric(df[col], errors="coerce") for col in column_list]
        result = cls._apply_expr(operand_cols, expr)
        mask = reduce(and_, (col.notna() for col in [result_column, *operand_cols]))
        mask = mask & ((result - result_column).abs() < tolerance)
        return mask

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, df, **kwargs):
        expr, tolerance, column_list = kwargs["expr"], kwargs["tolerance"], kwargs["column_list"]
        *operand_cols, result_column = [F.col(col).cast("double") for col in column_list]
        result = cls._apply_expr(operand_cols, expr)
        mask = reduce(and_, (col.isNotNull() for col in [result_column, *operand_cols]))
        mask = mask & (F.abs(result - result_column) < F.lit(tolerance))
        return mask

class ExpectColumnsArithmeticToEqualsResultColumn(MulticolumnMapExpectation):
    """
    验证多列经过指定表达式计算后是否等于结果列的值

    示例:
        expect_columns_arithmetic_to_equals_result_column(expr="{0}+{1}", column_list=["a", "b", "c"])
        验证 a + b == c（column_list参数最后一位固定是结果列）
        expect_columns_arithmetic_to_equals_result_column(expr="({0}+{1})*{2}-{3}", column_list=["a", "b", "c", "d", "e"], tolerance=1e-4)
        验证 (a + b) * c - d == e，允许等号两边的误差在0.0001内（考虑计算中的浮点精度误差问题）
    """
    map_metric = "column_arithmetic_equals_result_column"
    success_keys = ("mostly", "column_list", "expr", "ignore_row_if", "tolerance")
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "any_value_is_missing",
        "tolerance": 1e-2,
    }