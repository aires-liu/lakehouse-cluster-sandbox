from enum import Enum
import pandas as pd
from typing import List

class ParsedGXResult:
    def __init__(self, results):
        self.results = results

    class Fields(Enum):
        RULE = "验证规则"
        EXPECTATION = "期望类型"
        COLUMN_NAME = "验证字段"
        UNEXPECTED_VALUE = "错误值"
        UNEXPECTED_INDEX_LIST = "行索引"
        PASS = "是否通过"

    def get_dataframe(self) -> pd.DataFrame:
        results_data = self._extract_data_from_gx_result()
        df = pd.DataFrame(results_data, columns=[field.value for field in self.Fields][:-1]) 
        # 这里读取的UNEXPECTED_INDEX_LIST列类型默认是float64，实际调用时可能需要注意一下
        df[self.Fields.PASS.value] = df.groupby(self.Fields.RULE.value)[self.Fields.UNEXPECTED_INDEX_LIST.value]\
                                        .transform(lambda x: x.isnull().all())
        df.sort_values(by=[self.Fields.RULE.value, self.Fields.UNEXPECTED_INDEX_LIST.value], inplace=True)
        return df

    def _extract_data_from_gx_result(self) -> List[dict]:
        gx_result = self.results
        raw_results = []
        for result in gx_result["results"]:
            raw_result = self._extract_raw_gx_result_info(result)
            raw_results.append(raw_result)
        results_data = self._flatten_raw_results(raw_results)
        return results_data

    def _extract_raw_gx_result_info(self, result: dict) -> dict:
        expectation_config = result["expectation_config"]
        result_data = result["result"]
        raw_result = {
            self.Fields.RULE: expectation_config["meta"]["Rule"],
            self.Fields.EXPECTATION: expectation_config["expectation_type"],
            self.Fields.COLUMN_NAME: expectation_config["kwargs"]["column"],
            self.Fields.UNEXPECTED_VALUE: result_data["unexpected_list"],
            self.Fields.UNEXPECTED_INDEX_LIST: [item["index"] for item in result_data["unexpected_index_list"]]
        }
        return raw_result

    def _flatten_raw_results(self, raw_results: List[dict]) -> List[list]:
        flattened_results = []
        for raw_result in raw_results:
            rule: str = raw_result[self.Fields.RULE]
            expectation: str = raw_result[self.Fields.EXPECTATION]
            column_name: str = raw_result[self.Fields.COLUMN_NAME]
            unexpected_list: list = raw_result[self.Fields.UNEXPECTED_VALUE]
            if unexpected_list:
                for index, value in enumerate(unexpected_list):
                    flattened_results.append([rule, expectation, column_name, value, \
                                              raw_result[self.Fields.UNEXPECTED_INDEX_LIST][index]])
            else:
                flattened_results.append([rule, expectation, column_name, None, None])
        return flattened_results