import argparse
from datetime import datetime
from pyspark.sql import SparkSession

run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

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

def main():
    args = parse_args()
    spark = build_spark_session(args)
    # 1. 创建 Namespace (对应 Hive Database)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke_test")
    # 2. 创建/替换 Iceberg 表
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.smoke_test.orders (
            order_id   BIGINT    COMMENT '订单 ID',
            order_date STRING    COMMENT '下单日期',
            amount     DOUBLE    COMMENT '订单金额',
            status     STRING    COMMENT '订单状态'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    # 3. 写入第一批测试数据
    spark.sql("""
        INSERT INTO lakehouse.smoke_test.orders VALUES
            (1, '2024-01-01', 100.00, 'completed'),
            (2, '2024-01-02', 200.50, 'pending'),
            (3, '2024-01-03',  50.00, 'cancelled')
    """)
    # 4. 读取并验证行数
    df = spark.table("lakehouse.smoke_test.orders")
    rows = df.collect() # 这会把全量数据拉到 Driver，适合小数据量的表
    df.show() # 此时数据已经在内存中所以不需要再次全量读取数据
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
    # 5. 验证快照（Iceberg ACID 核心特性）
    snapshots_df = spark.sql(
        "SELECT snapshot_id, committed_at, operation "
        "FROM lakehouse.smoke_test.orders.snapshots"
    )
    snapshots_df.show(truncate=False)
    # 6. 写入第二批数据，然后做 Time Travel 回溯到上一版本
    spark.sql("""
        INSERT INTO lakehouse.smoke_test.orders VALUES
            (4, '2024-01-04', 300.00, 'completed')
    """)
    spark.table("lakehouse.smoke_test.orders").show()
    all_snapshots = spark.sql(
        "SELECT snapshot_id FROM lakehouse.smoke_test.orders.snapshots "
        "ORDER BY committed_at ASC"
    ).collect()
    first_snapshot_id = all_snapshots[-2]["snapshot_id"]
    print(f"Time Travel to snapshot_id: {first_snapshot_id}")
    time_travel_df = spark.read.option("snapshot-id", first_snapshot_id).table("lakehouse.smoke_test.orders")
    time_travel_df.show()
    spark.stop()

if __name__ == "__main__":
    main()
