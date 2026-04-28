import pandas as pd
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics import ColumnMapMetricProvider, column_condition_partial
import pyspark.sql.functions as F
from pyspark.sql.column import Column as SparkColumn
import pendulum
import operator as op

class ColumnValuesMeetDateCondition(ColumnMapMetricProvider):
    condition_metric_name = "column_values.meet_date_condition"
    condition_value_keys = ("date", "operator")

    @staticmethod
    def _get_date_operator(col_dates, date, operator):
        ops = {
            ">=": op.ge,
            "<=": op.le,
            ">": op.gt,
            "<": op.lt,
            "==": op.eq
        }
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator}. Supported operators are: {', '.join(ops.keys())}")
        return ops[operator](col_dates, date)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, date: str, operator: str, **kwargs):
        date = pd.to_datetime(date)
        col_dates = pd.to_datetime(column, format="%Y-%m-%d", errors="coerce")
        mask = col_dates.notna() & cls._get_date_operator(col_dates, date, operator)
        return mask

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column: SparkColumn, date: str, operator: str, **kwargs):
        date = F.to_date(F.lit(date), "yyyy-MM-dd")
        col_dates = F.to_date(column, "yyyy-MM-dd")
        mask = col_dates.isNotNull() & cls._get_date_operator(col_dates, date, operator)
        return mask
    
class ExpectColumnValuesToMeetDateCondition(ColumnMapExpectation):
    """
    验证列值是否符合YYYY-MM-DD格式并且满足指定的日期条件

    - 默认日期为香港当前日期
    - 默认比较符为"==", 支持">=", "<=", ">", "<", "=="
    - 支持验证pandas和Spark DataFrame

    示例:
        expect_column_values_to_meet_date_condition(column="date_column")
        验证当前列的日期值是否等于香港当前日期
        expect_column_values_to_meet_date_condition(column="date_column", date="2024-09-08")
        验证当前列的日期值是否等于2024-09-08
        expect_column_values_to_meet_date_condition(column="date_column", date="2024-09-08", operator=">=")
        验证当前列的日期值是否大于等于2024-09-08
    """
    map_metric = "column_values.meet_date_condition"
    success_keys = ("mostly", "date", "operator")
    default_kwarg_values = {
        "mostly": 1.0,
        "date": pendulum.now("Asia/Hong_Kong").date().to_date_string(),
        "operator": "=="
    }
