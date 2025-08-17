FROM minio/mc:latest
COPY minio/lottery_results.duckdb /lottery_results.duckdb

ENTRYPOINT [ "sh", "-c", "\
  sleep 5 && \
  /usr/bin/mc alias set myminio http://lottery-pipeline-minio:17110 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" && \
  /usr/bin/mc mb --region=\"$MINIO_REGION\" myminio/\"$MINIO_BUCKETS\" || true && \
  touch /tmp/.keep && \
  /usr/bin/mc cp /tmp/.keep myminio/\"$MINIO_BUCKETS\"/raw-results/.keep || true && \
  /usr/bin/mc cp /lottery_results.duckdb myminio/\"$MINIO_BUCKETS\"/lottery_results.duckdb || true \
"]