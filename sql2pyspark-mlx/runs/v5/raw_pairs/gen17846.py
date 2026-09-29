from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("level").orderBy("retry_count", "id")
w3 = Window.orderBy(F.col("has_fault").desc(), "id")
w_part = Window.partitionBy("level")

parsed = (
    df_app_logs
    .filter(F.col("status_code") > 364)
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .filter(F.col("level") == "INFO")
    .select(
        "*",
        F.rank().over(w).alias("latency_ms_rank"),
        F.ntile(3).over(w2).alias("retry_count_bucket"),
        F.ntile(5).over(w3).alias("has_fault_bucket"),
        F.count("status_code").over(w_part).alias("level_count_status_code"),
    )
)
