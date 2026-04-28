import argparse
import json
from datetime import datetime

import boto3
from pyspark.sql import SparkSession

run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

RESULT_BUCKET = "test-bucket"
RESULT_KEY_PREFIX = "output"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint_url", required=True)
    parser.add_argument("--aws_access_key_id", required=True)
    parser.add_argument("--aws_secret_access_key", required=True)
    return parser.parse_args()

def build_spark_session(args):
    """
    构建启用 Iceberg Hive Catalog + S3FileIO 的 SparkSession
    Catalog 名称: lakehouse
    Warehouse 路径: s3a://warehouse/
    """
    return (
        SparkSession.builder
            .appName("iceberg_smoke_test")
            # S3A (Hadoop FileSystem, 用于 spark.read/write 直接访问 MinIO)
            # 这里不涉及 Spark 读写 MinIO，所以 S3A 的配置可以简化，由 Iceberg 的 S3FileIO 管理 S3 访问
            # .config("spark.hadoop.fs.s3a.endpoint", args.endpoint_url)
            # .config("spark.hadoop.fs.s3a.access.key", args.aws_access_key_id)
            # .config("spark.hadoop.fs.s3a.secret.key", args.aws_secret_access_key)
            # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.executorEnv.AWS_REGION", "us-east-1")
            # Iceberg Extensions
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # Iceberg Hive Catalog (catalog 名称: lakehouse)
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hive")
            .config("spark.sql.catalog.lakehouse.uri", "thrift://hive:9083")
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/")
            # S3FileIO: 由 Iceberg 自己管理 S3 读写（不走 S3A）
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.s3.endpoint", args.endpoint_url)
            .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
            # S3FileIO 使用 AWS SDK v2，需要单独传入凭证（不复用 S3A 的 Hadoop 凭证）
            .config("spark.sql.catalog.lakehouse.s3.access-key-id", args.aws_access_key_id)
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key", args.aws_secret_access_key)
            .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
            # AWS SDK v2 HTTP client，配合 bundle-2.17.230.jar
            .config("spark.sql.catalog.lakehouse.s3.http-client-type", "urlconnection")
            .getOrCreate()
    )

def write_result_to_minio(args, result: dict):
    key = f"{RESULT_KEY_PREFIX}/iceberg_smoke_test_{run_timestamp}.json"
    client = boto3.client(
        service_name="s3",
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key,
    )
    client.put_object(
        Bucket=RESULT_BUCKET,
        Key=key,
        Body=json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[OK] Smoke test result written to s3://{RESULT_BUCKET}/{key}")

def main():
    args = parse_args()
    spark = build_spark_session(args)

    # ── Step 1: 创建 Namespace (对应 Hive Database) ───────────────────────────────
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke_test")
    print("[OK] Step 1 — Namespace created: lakehouse.smoke_test")

    # ── Step 2: 创建/替换 Iceberg 表 ──────────────────────────────────────────────
    # 使用 CREATE OR REPLACE 保证每次运行幂等，不受残留数据影响
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.smoke_test.orders (
            order_id   BIGINT    COMMENT '订单 ID',
            order_date STRING    COMMENT '下单日期 YYYY-MM-DD',
            amount     DOUBLE    COMMENT '订单金额',
            status     STRING    COMMENT '订单状态'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print("[OK] Step 2 — Iceberg table created: lakehouse.smoke_test.orders")

    # ── Step 3: 写入测试数据（第一批） ────────────────────────────────────────────
    spark.sql("""
        INSERT INTO lakehouse.smoke_test.orders VALUES
            (1, '2024-01-01', 100.00, 'completed'),
            (2, '2024-01-02', 200.50, 'pending'),
            (3, '2024-01-03',  50.00, 'cancelled')
    """)
    print("[OK] Step 3 — Inserted 3 rows (first batch)")

    # ── Step 4: 读取并验证行数 ─────────────────────────────────────────────────────
    df = spark.table("lakehouse.smoke_test.orders")
    rows = df.collect()
    print(f"[OK] Step 4 — Read back {len(rows)} rows:")
    df.show()
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

    # ── Step 5: 验证快照（Iceberg ACID 核心特性） ─────────────────────────────────
    snapshots_df = spark.sql(
        "SELECT snapshot_id, committed_at, operation "
        "FROM lakehouse.smoke_test.orders.snapshots"
    )
    snapshots = snapshots_df.collect()
    print(f"[OK] Step 5 — Snapshot count: {len(snapshots)}")
    snapshots_df.show(truncate=False)

    # ── Step 6: 再写入一批，然后做时光回溯（Time Travel） ─────────────────────────
    spark.sql("""
        INSERT INTO lakehouse.smoke_test.orders VALUES
            (4, '2024-01-04', 300.00, 'completed')
    """)
    # 获取第一个快照 ID（CREATE OR REPLACE 后第一次 INSERT 产生的快照）
    all_snapshots = spark.sql(
        "SELECT snapshot_id FROM lakehouse.smoke_test.orders.snapshots "
        "ORDER BY committed_at ASC"
    ).collect()

    first_snapshot_id = all_snapshots[0]["snapshot_id"]
    time_travel_df = spark.read.option("snapshot-id", first_snapshot_id).table(
        "lakehouse.smoke_test.orders"
    )
    time_travel_count = time_travel_df.count()
    print(
        f"[OK] Step 6 — Time travel to snapshot {first_snapshot_id}: "
        f"{time_travel_count} rows (expected 3)"
    )
    time_travel_df.show()

    # ── 汇总 ──────────────────────────────────────────────────────────────────────
    result = {
        "status": "success",
        "run_timestamp": run_timestamp,
        "catalog": "lakehouse (HiveCatalog, s3a://warehouse/)",
        "table": "lakehouse.smoke_test.orders",
    }
    print("\n=== Iceberg Smoke Test PASSED ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    write_result_to_minio(args, result)
    spark.stop()


if __name__ == "__main__":
    main()
