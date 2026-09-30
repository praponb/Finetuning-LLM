from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("retry_count", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .filter(F.col("is_flagged") > 0)
    .select(
        "id",
        "log_ts",
        "status_code",
        "is_flagged",
        "retry_count",
        F.max("status_code").over(w).alias("last4_max_status_code"),
        F.count("status_code").over(w).alias("last4_count_status_code"),
        F.sum("status_code").over(w).alias("last4_sum_status_code"),
        F.avg("status_code").over(w).alias("last4_avg_status_code"),
    )
)
