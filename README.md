# Lakehouse Cluster Sandbox

这是一个可独立运行的湖仓一体学习集群沙箱，目录内已包含所需配置、脚本和测试数据。

## 技术架构总览

| 架构层 | 组件 | 版本/镜像 | 主要职责 | 关键关系 |
| --- | --- | --- | --- | --- |
| 计算层 | Spark Master + Worker | bitnami/spark:3.3.0 | 执行批处理与数据校验任务 | 读取/写入 MinIO，按参数可启用 Iceberg Catalog |
| 编排层 | Airflow Webserver + Scheduler | custom-airflow:sandbox (Airflow 2.7.1) | 调度 DAG、触发 Spark 作业、管理变量与连接 | 通过 spark_default 提交到 Spark，依赖 PostgreSQL 元数据库 |
| 元数据层 | Hive Metastore + PostgreSQL | custom-hive:sandbox + postgres:13 | 保存表元数据与 metastore schema | 供 Spark/Iceberg(Hive Catalog) 访问元数据 |
| 存储层 | MinIO | custom-minio:sandbox | 提供 S3 兼容对象存储（测试数据与结果） | Warehouse 与输入输出数据落地位置 |
| 表格式层 | Iceberg（运行时启用） | iceberg-spark-runtime-3.3_2.12:1.1.0 | 提供 ACID、快照、时光回溯等表能力 | 通过 Spark 提交参数注入 Extensions + Catalog |
| 数据质量层 | Great Expectations | great_expectations==0.18.12 | 规则校验与结果产出 | 在 Airflow/Spark 任务中执行并写回 MinIO |

## 1. 项目目标

- 一键拉起 Spark、Airflow、Hive Metastore、MinIO、PostgreSQL
- 使用 MinIO 作为对象存储，支持 Spark S3A 读写
- 支持通过 Spark 提交参数启用 Iceberg（Hive Catalog）实现湖上表管理与 ACID 能力
- 提供 Airflow 测试 DAG 与 Great Expectations 自动化校验脚本
- 便于本地学习与实验，尽量避免端口冲突与外网下载慢问题

## 2. 服务组成

- Spark
   - `spark-master` (bitnami/spark:3.3.0)
   - `spark-worker` (bitnami/spark:3.3.0)
- PostgreSQL
   - `postgres` (postgres:13)
   - 初始化时创建 `airflow` 与 `hive_metastore` 数据库和对应用户
- Airflow
   - `airflow-webserver` (custom-airflow:sandbox)
   - `airflow-scheduler` (custom-airflow:sandbox)
- Hive
   - `hive` (custom-hive:sandbox)
   - 启动时自动检查并初始化/升级 metastore schema
- MinIO
   - `minio` (custom-minio:sandbox)
   - 启动时自动创建 bucket 并导入测试数据

## 3. 关键特性

### 3.1 随机端口映射

`docker-compose.yaml` 中端口使用了只写容器端口的形式（如 `"8080"`、`"9000"`），Docker 会自动分配宿主机端口，减少本机端口冲突。

启动后请通过以下命令查看实际端口：

```bash
docker compose ps
```

### 3.2 国内镜像源下载

已在镜像构建中配置国内源，主要包括：

- Debian apt 源: 清华镜像
- Python pip 源: 清华镜像
- Maven 依赖源: 阿里云镜像
- Spark 二进制下载: 华为云镜像

### 3.3 Iceberg 启用方式

本项目镜像内已包含 Iceberg Spark Runtime 依赖。  
Iceberg 的真正启用方式是在 Spark 提交任务时通过参数注入 Catalog 与 Extensions（而不是仅靠 Hive 配置文件）。

建议在 Spark 提交时增加以下关键配置（示例）：

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=hive
spark.sql.catalog.lakehouse.uri=thrift://hive:9083
spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/
spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000
spark.sql.catalog.lakehouse.s3.path-style-access=true
```

说明：

- 如果不传上述 Spark 参数，即使镜像中已有 Iceberg jar，也不会自动按 Iceberg 表格式管理元数据
- 你当前项目中的 Hive Metastore 与 MinIO 配置可直接作为 Iceberg Hive Catalog 的后端依赖

## 4. 快速开始

### 4.1 环境要求

- Docker + Docker Compose（推荐 Docker Desktop 最新稳定版）
- 可用网络（首次构建会下载基础镜像与依赖）

### 4.2 启动集群

在当前目录执行：

```bash
docker compose up -d --build
```

查看状态：

```bash
docker compose ps
```

### 4.3 访问入口

以下是容器内部端口，宿主机端口以 `docker compose ps` 输出为准：

- Spark UI: `spark-master:8080`
- Airflow UI: `airflow-webserver:8080`
- MinIO API: `minio:9000`
- MinIO Console: `minio:9090`

默认账号密码：

- Airflow: `admin / password`
- MinIO: `admin / password`

## 5. 启动时自动初始化行为

### 5.1 PostgreSQL

执行 `cluster_resources/postgres_utils/init_postgres.sql`：

- 创建数据库 `airflow`
- 创建数据库 `hive_metastore`
- 创建用户 `airflow`、`hive` 并授权

### 5.2 Airflow

`cluster_resources/airflow_utils/entrypoint.sh` 会执行：

- `airflow db migrate`
- 首次启动自动创建管理员 `admin`
- 写入 Airflow Variables（MinIO endpoint 与 AK/SK）
- 创建/刷新 `spark_default` 连接

### 5.3 Hive

`cluster_resources/hive_utils/hive-entrypoint.sh` 会：

- 检查 metastore schema 是否存在
- 不存在则 `initSchema`
- 存在则 `upgradeSchema`
- 启动 Hive Metastore 服务

### 5.4 MinIO

`cluster_resources/minio_utils/minio-init.sh` 会：

- 启动 MinIO Server
- 创建 bucket: `test-bucket`、`warehouse`
- 将 `dags/test_gx/test_data/` 下数据复制到 `test-bucket/input/`

## 6. 自动化测试

自动化测试总入口 DAG：`dags/test_airflow/test_dag/auto_testing.py`

执行逻辑：

1. 自动触发同目录下 `test_dag_XX.py`（当前匹配 `test_dag_01` 到 `test_dag_09`）
2. 这些 DAG 执行完成后，运行 `dags/test_gx/test_scripts/auto_testing.py`
3. `auto_testing.py` 会顺序执行同目录下全部 `test_*.py` Great Expectations 测试脚本

其中部分 Spark + GX 任务（如 `gx_validate_minio.py`）会将校验结果写回 MinIO：

- bucket: `test-bucket`
- prefix: `output/`

### 6.1 触发方式

- 方式一：在 Airflow UI 手动触发 DAG `auto_testing`
- 方式二：命令行触发

```bash
docker compose exec airflow-webserver airflow dags trigger auto_testing
```

### 6.2 结果查看

- Airflow UI 查看各 Task 日志与状态
- MinIO Console 查看 `test-bucket/output/` 输出文件
- 本地示例结果可参考 `dags/test_gx/validation_result/`

### 6.3 Iceberg 验收建议

当你在 Spark 提交参数中启用 Iceberg 后，建议增加一条最小验收链路：

1. 建库建表（`USING iceberg`）
2. 插入/更新数据（验证写入与快照生成）
3. 执行 time travel 查询（如 `VERSION AS OF`）验证快照可回溯

## 7. 常用运维命令

查看服务状态：

```bash
docker compose ps
```

查看日志（示例）：

```bash
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f hive
docker compose logs -f minio
```

停止服务：

```bash
docker compose down
```

停止并清理数据卷：

```bash
docker compose down -v
```

## 8. 目录说明

- `docker-compose.yaml`: 集群编排
- `dockerfile.airflow`: Airflow + Spark + Python 依赖构建
- `dockerfile.hive`: Hive 及依赖构建
- `dockerfile.minio`: MinIO 与初始化工具构建
- `cluster_resources/`: 各服务初始化脚本与配置
- `dags/`: Airflow DAG、Spark 作业、GX 自定义期望、测试数据与测试脚本

## 9. 说明

- 本目录是完整沙箱，正常使用不需要修改目录外文件
- 新增 DAG 直接放在 `dags/` 下即可被 Airflow 挂载识别
