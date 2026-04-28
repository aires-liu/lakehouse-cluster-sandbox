from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_validation_result import ExpectationSuiteValidationResult
from great_expectations.data_context import AbstractDataContext
from great_expectations.validator.validator import Validator
from typing import Union, List
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def execute_gx_validation(
    task_id: str,
    expectation_config: List[ExpectationConfiguration],
    batch_request: BatchRequest,
    result_format: Union[dict, str, None],
    gx_context: AbstractDataContext,
) -> ExpectationSuiteValidationResult:
    suite = ExpectationSuite(expectation_suite_name=task_id)
    for config in expectation_config:
        suite.add_expectation(config)
    validator: Validator = gx_context.get_validator(batch_request=batch_request,
                                                    expectation_suite=suite)
    return validator.validate(result_format=result_format or "BASIC")