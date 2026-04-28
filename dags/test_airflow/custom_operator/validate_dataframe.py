from typing import Union, List, Callable
from airflow.models import BaseOperator
from test_airflow.custom_operator.common import execute_gx_validation
from airflow.utils.context import Context
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame
import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

class GXValidateDataFrameOperator(BaseOperator):
    template_fields = ("loader_kwargs", "writer_kwargs") # 声明需要Jinja模板渲染的变量
    def __init__(
        self,
        dataframe_loader: Callable[..., Union[PandasDataFrame, SparkDataFrame]],
        result_writer: Callable[..., None],
        expectation_config: dict,
        result_format: Union[dict, str] = None,
        loader_kwargs=None,
        writer_kwargs=None,
        *args,
        **kwargs,
    ) -> dict:
        super().__init__(*args, **kwargs)
        self.dataframe_loader = dataframe_loader
        self.result_writer = result_writer
        self.expectation_config = self._parse_expectation_config(expectation_config)
        self.result_format = result_format
        self.loader_kwargs = loader_kwargs or {}
        self.writer_kwargs = writer_kwargs or {}

    def execute(self, context: Context) -> dict:
        dataframe = self.dataframe_loader(**self.loader_kwargs)
        gx_context = gx.get_context()
        if isinstance(dataframe, PandasDataFrame):
            batch_request = self._get_pandas_batch(gx_context, dataframe)
        elif isinstance(dataframe, SparkDataFrame):
            batch_request = self._get_spark_batch(gx_context, dataframe)
        else:
            raise ValueError(f"Unsupported dataframe type: {type(dataframe).__name__}")
        result = execute_gx_validation(
            task_id=self.task_id,
            expectation_config=self.expectation_config,
            batch_request=batch_request,
            result_format=self.result_format,
            gx_context=gx_context,
        )
        self.result_writer(result, **self.writer_kwargs)
        return result.to_json_dict()

    def _get_spark_batch(
            self, 
            gx_context: AbstractDataContext, 
            dataframe: SparkDataFrame
        ) -> BatchRequest:
        return (gx_context.sources.add_or_update_spark(name=self.task_id)
                .add_dataframe_asset(name=self.task_id, dataframe=dataframe)
                .build_batch_request())

    def _get_pandas_batch(
            self, 
            gx_context: AbstractDataContext, 
            dataframe: PandasDataFrame
        ) -> BatchRequest:
        return (gx_context.sources.add_or_update_pandas(name=self.task_id)
                .add_dataframe_asset(name=self.task_id, dataframe=dataframe)
                .build_batch_request())

    def _parse_expectation_config(self, expectation_config: dict) -> List[ExpectationConfiguration]:
        return [ExpectationConfiguration(expectation_type=item["expectation_type"], kwargs=item["kwargs"])
                for item in expectation_config]
