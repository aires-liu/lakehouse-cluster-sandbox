import pandas as pd
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.metrics import ColumnMapMetricProvider, column_condition_partial
import pyspark.sql.functions as F
from pyspark.sql.column import Column as SparkColumn

class ColumnValuesMatchDateFormat(ColumnMapMetricProvider):
    condition_metric_name = "column_values.match_date_format"
    condition_value_keys = ("date_format",)
    filter_column_isnull = False

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, date_format: str, **kwargs):
        cls._validate_format(date_format)
        date_format = date_format.replace("YYYY", "%Y").replace("MM", "%m").replace("DD", "%d")
        col_dates = pd.to_datetime(column, format=date_format, errors="coerce")
        mask = col_dates.notna()
        return mask

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column: SparkColumn, date_format: str, **kwargs):
        cls._validate_format(date_format)
        date_format = date_format.replace("YYYY", "yyyy").replace("DD", "dd")
        regex_map = {
            "yyyy-MM-dd": r"^\d{4}-\d{2}-\d{2}$",
            "yyyy-MM": r"^\d{4}-\d{2}$",
            "yyyy": r"^\d{4}$"
        } 
        # Spark 3.x中的to_date日期解析严格
        # 比如在指定YYYY-MM格式解析时必须确保没有YYYY-MM-DD格式的数据
        # 否则会直接报错，所以这里采用正则进行一遍过滤
        mask = F.when(column.rlike(regex_map[date_format]), \
                      F.to_date(column, date_format).isNotNull()).otherwise(F.lit(False))
        return mask

    @staticmethod
    def _validate_format(date_format: str):
        if date_format not in ["YYYY-MM-DD", "YYYY-MM", "YYYY"]:
            raise ValueError(f"date_format only supports 'YYYY-MM-DD', 'YYYY-MM', 'YYYY'.\
                              The input format is: '{date_format}'")
    
class ExpectColumnValuesToMatchDateFormat(ColumnMapExpectation):
    """
    验证列值是否符合指定的日期格式

    - 默认格式为YYYY-MM-DD，支持YYYY-MM和YYYY格式
    - 支持验证pandas和Spark DataFrame

    示例:
        expect_column_values_to_match_date_format(column="date_column")
        验证当前列的值是否符合YYYY-MM-DD格式
        expect_column_values_to_match_date_format(column="date_column", date_format="YYYY-MM")
        验证当前列的值是否符合YYYY-MM格式
        expect_column_values_to_match_date_format(column="date_column", date_format="YYYY")
        验证当前列的值是否符合YYYY格式

    其他:
        如果需要在result的kwargs中显示参数的值比如date和operator需要显式传入参数而不是使用默认值
    """
    map_metric = "column_values.match_date_format"
    success_keys = ("mostly", "date_format")
    default_kwarg_values = {
        "mostly": 1.0,
        "date_format": "YYYY-MM-DD"
    }

# # 正则表达式验证日期格式的有效性和日期的合法性
# # Matching Year YYY: Excludes 0000, supported 8001~9999
# year = r"(?:\d{3}[1-9]1\d{2}[1-9]\d{1}|\d{1}[1-9]\d{2}|[1-9]\d{3})"
# # Match with MM-DD (01,03,05,07,08,10,12) for months since they all have 31 days.
# big_month =r"(?:0[13578]|1[02])-(?:0[1-9]|[12]\d|3[01])"
# # Match with MM-DD (04,06,09,11) for small months since they all have 30 days.
# small_month =r"(?:0[469]|11)-(?:0[1-9]|[12]\d|30)"
# # Match with MM-DD(02) for February(up to 28th)
# feb_normal_month=r"02-(?:0[1-9]|1\d|2[8-8])"
# # Merge year date M-D
# normal_date = rf"(?:{big_month}|{small_month}|{feb_normal_month})"
# # Leap year rule (divisible by 4 but not by 100, or divisible by 400)
# leap_year = r"(?:(\d{2}(0[48]|[2468][048]|[13579][26]))|((0[48]|[2468][848]|[3579][26])68))"
# # Leap year 2-29 valid date
# leap_date = r"02-29"
# # Integrate common year and leap year rules
# full_regex = rf"(?:{year}-{normal_date}|{leap_year}-{leap_date})"
