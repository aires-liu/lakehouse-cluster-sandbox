import pendulum
import pandas as pd
from great_expectations.expectations.expectation import Expectation
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as F

class ExpectColumnValuesToMeetDateConditionV1(Expectation):
    """
    验证列值是否符合YYYY-MM-DD格式并且满足指定的日期条件

    - 默认日期为香港当前日期
    - 默认比较符为"==", 支持">=", "<=", ">", "<", "=="
    - 支持验证pandas和Spark DataFrame

    示例:
        expect_column_values_to_meet_date_condition_v1(column="date_column")
        验证当前列的日期值是否等于香港当前日期
        expect_column_values_to_meet_date_condition_v1(column="date_column", date="2024-09-08")
        验证当前列的日期值是否等于2024-09-08
        expect_column_values_to_meet_date_condition_v1(column="date_column", date="2024-09-08", operator=">=")
        验证当前列的日期值是否大于等于2024-09-08
    """
    metric_dependencies = ()
    success_keys = ("column", "mostly")

    def _validate(self, configuration, metrics, runtime_configuration=None, execution_engine=None):
        column_name = configuration.kwargs["column"]
        default_date = pendulum.now("Asia/Hong_Kong").date().to_date_string()
        date: str = configuration.kwargs.get("date", default_date) # 默认为香港的当天日期
        operator = configuration.kwargs.get("operator", "==")
        mostly = configuration.kwargs.get("mostly", 1.0)
        df = execution_engine.dataframe
        # 根据传入的DataFrame类型选择不同的处理方式
        if isinstance(df, pd.DataFrame):
            success, element_count, unexpected_count, unexpected_percent, \
            unexpected_list, unexpected_index_list = self._pandas_grammar(df, column_name, mostly, date, operator)
        elif isinstance(df, SparkDataFrame):
            success, element_count, unexpected_count, unexpected_percent, \
            unexpected_list, unexpected_index_list = self._spark_grammar(df, column_name, mostly, date, operator)
        else:
            raise TypeError("Unsupported data type. Supported types are: pandas.DataFrame, SparkDataFrame.")
        # 返回验证结果
        return {
            "success": success,
            "result": {
                "element_count": element_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": unexpected_percent,
                "unexpected_list": unexpected_list,
                "unexpected_index_list": unexpected_index_list,
            }
        }
    
    def _pandas_grammar(self, df: pd.DataFrame, column_name: str, mostly: float, date: str, operator: str):
        col_dates = pd.to_datetime(df[column_name], format="%Y-%m-%d", errors="coerce")
        date = pd.to_datetime(date)
        date_operator = self._get_date_operator(col_dates, date, operator)
        condition_mask = col_dates.notna() & date_operator # 101100011...
        # 统计满足条件的比例
        expected_percent = condition_mask.mean()
        success = expected_percent >= mostly
        unexpected_percent = round((1 - expected_percent) * 100, 1)
        element_count = len(df)
        # 获取不满足条件的值和索引
        unexpected_list = df.loc[~condition_mask, column_name].tolist()
        unexpected_count = len(unexpected_list)
        unexpected_index_list = df.index[~condition_mask].tolist()
        return success, element_count, unexpected_count, unexpected_percent, unexpected_list, unexpected_index_list
    
    def _spark_grammar(self, df: SparkDataFrame, column_name: str, mostly: float, date: str, operator: str):
        col_dates = F.to_date(F.col(column_name), "yyyy-MM-dd")
        date = F.to_date(F.lit(date), "yyyy-MM-dd")
        date_operator = self._get_date_operator(col_dates, date, operator)
        condition_mask = (col_dates.isNotNull()) & date_operator # 101100011...
        # 统计满足条件的比例
        expected_percent = df.filter(condition_mask).count() / df.count()
        success = expected_percent >= mostly
        unexpected_percent = round((1 - expected_percent) * 100, 1)
        element_count = df.count()
        # 获取不满足条件的值和索引（使用 index 列）
        unexpected_df = df.filter(~condition_mask)
        unexpected_rows = unexpected_df.select(column_name, "index").collect()
        unexpected_list = [row[column_name] for row in unexpected_rows]
        unexpected_count = len(unexpected_list)
        unexpected_index_list = [row["index"] for row in unexpected_rows]
        return success, element_count, unexpected_count, unexpected_percent, unexpected_list, unexpected_index_list
    
    @staticmethod
    def _get_date_operator(col_dates, date, operator):
        ops = {
            ">=": col_dates >= date,
            "<=": col_dates <= date,
            ">": col_dates > date,
            "<": col_dates < date,
            "==": col_dates == date
        }
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator}. Supported operators are: {', '.join(ops.keys())}")
        return ops[operator]
