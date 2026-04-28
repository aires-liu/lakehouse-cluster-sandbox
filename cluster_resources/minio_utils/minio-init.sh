#!/bin/bash
set -e

minio server /data --console-address ":9090" &
MINIO_PID=$!

for i in {1..5}; do
  if mc alias set minio-dev http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; then
    break
  fi
  sleep 2
done

mc alias set minio-dev http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

buckets=("test-bucket" "warehouse")
for bucket in "${buckets[@]}"; do
  if ! mc ls "minio-dev/$bucket" >/dev/null 2>&1; then
    mc mb "minio-dev/$bucket"
  fi
done

mc cp --recursive /test_data/* minio-dev/test-bucket/input/

wait "$MINIO_PID"
