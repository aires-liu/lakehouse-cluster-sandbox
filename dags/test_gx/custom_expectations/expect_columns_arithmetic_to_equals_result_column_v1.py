import pandas as pd
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import MulticolumnMapMetricProvider, multicolumn_condition_partial
import pyspark.sql.functions as F
import operator as op
from pandas import Series

class ColumnArithmeticEqualsResultV1(MulticolumnMapMetricProvider):
    condition_metric_name = "column_arithmetic_equals_result_column_v1" # 注意metric冲突
    # 这里和继承ColumnPairMapMetricProvider不同，列参数名不再强制为column_A、column_B，只需要确保传递一致即可
    condition_value_keys = ("operator", "left_column", "right_column", "result_column", "tolerance")
    # 指定哪些参数会被传递给底层的条件计算方法（如 _pandas、_spark）

    @staticmethod
    def _apply_operator(col_A, col_B, operator_str):
        ops = {
            "+": op.add,
            "-": op.sub,
            "*": op.mul,
            "/": op.truediv
        }
        if operator_str not in ops:
            raise ValueError(f"Unsupported operator: {operator_str}")
        return ops[operator_str](col_A, col_B)

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, df: pd.DataFrame, **kwargs):
        # _pandas方法中传递的df参数是整个Pandas DataFrame
        operator = kwargs["operator"]
        tolerance = kwargs["tolerance"]
        left_column: Series = pd.to_numeric(df[kwargs["left_column"]], errors="coerce")
        right_column: Series = pd.to_numeric(df[kwargs["right_column"]], errors="coerce")
        result_column: Series = pd.to_numeric(df[kwargs["result_column"]], errors="coerce")
        result = cls._apply_operator(left_column, right_column, operator)
        mask = left_column.notna() & right_column.notna() & result_column.notna() \
               & ((result - result_column).abs() < tolerance)
        return mask

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, df, **kwargs):
        # _spark方法中传递的df参数是一个列名字符串（不可删除），实际使用F.col来引用列
        # df: DataFrame[list_price: string, discount: double, sale_price: string]
        operator = kwargs["operator"]
        tolerance = kwargs["tolerance"]
        left_column = F.col(kwargs["left_column"]).cast("double")
        right_column = F.col(kwargs["right_column"]).cast("double")
        result_column = F.col(kwargs["result_column"]).cast("double")
        result = cls._apply_operator(left_column, right_column, operator)
        mask = left_column.isNotNull() & right_column.isNotNull() & result_column.isNotNull() \
               & (F.abs(result - result_column) < F.lit(tolerance))
        return mask

class ExpectColumnsArithmeticToEqualsResultColumnV1(MulticolumnMapExpectation):
    """
    验证两列经过指定运算符(+、-、*、/)后是否等于结果列的值

    示例:
        expect_columns_arithmetic_to_equals_result_column(left_column="a", right_column="b", result_column="c", operator="+", column_list=["a", "b", "c"])
        验证 a + b == c（注意需要加一个column_list参数）
        expect_columns_arithmetic_to_equals_result_column(left_column="a", right_column="b", result_column="c", operator="*", column_list=["a", "b", "c"], tolerance=1e-4)
        验证 a * b == c，允许误差在1e-4内
    """
    # 对于对于 MulticolumnMapExpectation 类型的自定义期望，column_list 是必须的
    # 用于框架内部确定哪些列会被传递给底层的条件计算方法（如 _pandas、_spark），并用于校验和报告
    map_metric = "column_arithmetic_equals_result_column_v1"
    success_keys = ("mostly", "left_column", "right_column", "result_column", "operator", "ignore_row_if", "tolerance")
    # 指定哪些参数会影响期望的“成功”判定和结果格式，这些参数会被记录在校验结果中，并用于配置和报告
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "any_value_is_missing", # 注意这个参数的值和 ExpectColumnPairValuesToBeEqual 的不同
        "tolerance": 1e-2,  # 设置一个容忍误差值来处理浮点数运算比较产生的精度问题
    }