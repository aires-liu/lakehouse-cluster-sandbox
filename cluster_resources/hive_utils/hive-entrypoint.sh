# #!/bin/bash

# 重写hive的启动脚本，避免使用默认的derby数据库
# 等待PostgreSQL启动完成后开始读取
echo "Waiting for PostgreSQL..."
sleep 5

echo "Checking if schema exists..."
/opt/hive/bin/schematool -dbType postgres -info
SCHEMA_EXISTS=$? # 拿到上一条命令的返回值
# 检查schema是否存在（避免容器重启后重复初始化导致冲突）
if [ $SCHEMA_EXISTS -eq 0 ]; then
    echo "Hive metastore schema already exists. Using existing schema."
    /opt/hive/bin/schematool -dbType postgres -upgradeSchema
else # 如果不存在（第一次启动hive容器需要初始化）
    echo "Initializing metastore with PostgreSQL..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
fi
# 启动Hive Metastore服务
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore
# 由于postgres的数据挂载本地，即使容器集群删除后重建，也能保证不产生冲突