import pandas as pd
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics import ColumnMapMetricProvider, column_condition_partial
import pyspark.sql.functions as F
from pyspark.sql.column import Column as SparkColumn

class ColumnValuesIsSpecificDay(ColumnMapMetricProvider):
    condition_metric_name = "column_values.is_specific_day"
    condition_value_keys = ("day_of_month", "day_of_week")

    @staticmethod
    def _validate_day_of_month(day_of_month):
        if not isinstance(day_of_month, int):
            raise TypeError("day_of_month must be an integer (positive for normal, negative for reverse index)")

    @staticmethod
    def _validate_day_of_week(day_of_week):
        if not isinstance(day_of_week, int):
            raise TypeError("day_of_week must be an integer between 1 and 7 (1=Monday, 7=Sunday)")
        if not (1 <= day_of_week <= 7):
            raise ValueError("day_of_week must be between 1 and 7 (1=Monday, 7=Sunday)")

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, day_of_month: int, day_of_week: int, **kwargs):
        col_dates = pd.to_datetime(column, format="%Y-%m-%d", errors="coerce")
        mask = col_dates.notna()
        if day_of_month is not None:
            cls._validate_day_of_month(day_of_month)
            if day_of_month > 0:
                mask &= (col_dates.dt.day == day_of_month)
            else:
                mask &= ((col_dates.dt.days_in_month - col_dates.dt.day + 1) == abs(day_of_month))
        if day_of_week is not None:
            cls._validate_day_of_week(day_of_week)
            pandas_weekday = (day_of_week - 1) % 7 # pandas的周一到周日是 0, 1, 2, 3, 4, 5, 6
            mask &= (col_dates.dt.weekday == pandas_weekday)
        return mask

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column: SparkColumn, day_of_month: int, day_of_week: int, **kwargs):
        col_date = F.to_date(column, "yyyy-MM-dd")
        mask = col_date.isNotNull()
        if day_of_month is not None:
            cls._validate_day_of_month(day_of_month)
            if day_of_month > 0:
                mask &= (F.dayofmonth(col_date) == F.lit(day_of_month))
            else:
                mask &= ((F.dayofmonth(F.last_day(col_date)) - F.dayofmonth(col_date) + 1) == F.lit(abs(day_of_month)))
        if day_of_week is not None:
            cls._validate_day_of_week(day_of_week)
            spark_weekday = day_of_week + 1 if day_of_week < 7 else 1 # spark的周一到周日是 2，3, 4, 5, 6, 7, 1
            mask &= (F.dayofweek(col_date) == F.lit(spark_weekday))
        return mask
    
class ExpectColumnValuesToBeSpecificDay(ColumnMapExpectation):
    """
    验证列值是否为当月第n天或当周第n天
    
    参数:
        day_of_month: 指定当月第几天（负数表示倒数第几天）
        day_of_week: 指定星期几（周一到周日为1, 2, 3, 4, 5, 6, 7）
    示例:
        expect_column_values_to_be_specific_day(column="date_column", day_of_month=31)
        expect_column_values_to_be_specific_day(column="date_column", day_of_week=1)
    """
    map_metric = "column_values.is_specific_day"
    success_keys = ("mostly", "day_of_month", "day_of_week")
    default_kwarg_values = {
        "mostly": 1.0,
        "day_of_month": None,
        "day_of_week": None
    }