import argparse
import json
import os
import sys
from datetime import datetime

import boto3
import great_expectations as gx
from great_expectations.datasource.fluent import BatchRequest, DataAsset, Datasource
from great_expectations.validator.validator import Validator
from pyspark.sql import SparkSession

run_timestamp = f"{datetime.now().strftime('%Y%m%d%H%M')}"

def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate a CSV file from MinIO with Great Expectations and write the result back to MinIO."
    )
    parser.add_argument("--endpoint_url", required=True)
    parser.add_argument("--aws_access_key_id", required=True)
    parser.add_argument("--aws_secret_access_key", required=True)
    parser.add_argument("--run_timestamp", default=run_timestamp)
    parser.add_argument("--bucket", default="test-bucket")
    parser.add_argument("--input_key", default="input/test_data_02.csv")
    parser.add_argument("--output_prefix", default="output")
    return parser.parse_args()


def build_spark_session(args):
    return (
        SparkSession.builder.appName("gx_validate_minio")
        .config("spark.hadoop.fs.s3a.endpoint", args.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", args.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", args.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_result_to_minio(args, result):
    key = f"{args.output_prefix}/gx_result_test_data_02_spark_{args.run_timestamp}.json"
    client = boto3.client(
        service_name="s3",
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
    )
    client.put_object(
        Bucket=args.bucket,
        Key=key,
        Body=json.dumps(result.to_json_dict(), ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"GX validation result written to s3://{args.bucket}/{key}")


def main():
    args = parse_args()

    dags_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.append(dags_dir)
    from test_gx import custom_expectations  # noqa: F401

    spark = build_spark_session(args)
    input_path = f"s3a://{args.bucket}/{args.input_key}"
    dataframe = spark.read.csv(input_path, header=True, inferSchema=True, multiLine=True)

    context = gx.get_context()
    data_source: Datasource = context.sources.add_or_update_spark(name="minio_spark_source")
    data_asset: DataAsset = data_source.add_dataframe_asset(name="test_data_02", dataframe=dataframe)
    batch_request: BatchRequest = data_asset.build_batch_request()

    suite_name = "minio_test_data_02_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    validator.expect_column_values_to_meet_date_condition(
        column="order_date",
        date="2024-09-07",
        operator=">=",
        meta={"Rule": "R0001"},
    )
    validator.expect_column_values_to_match_date_format(
        column="order_date",
        date_format="YYYY-MM-DD",
        meta={"Rule": "R0002"},
    )
    validator.expect_column_values_to_be_between(
        column="discount",
        min_value=0.2,
        meta={"Rule": "R0003"},
    )

    result = validator.validate(
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["order_id"],
            "return_unexpected_index_query": True,
        }
    )
    write_result_to_minio(args, result)
    spark.stop()


if __name__ == "__main__":
    main()
