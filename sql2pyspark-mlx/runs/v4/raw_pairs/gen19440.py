from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy("status_code_high")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "value",
        "status_code_high",
        "is_flagged",
        F.trim("key").alias("clean_key"),
    )
)

metrics = (
    valid
    .withColumn("status_code_high_max_1h", F.max("status_code_high").over(w))
    .withColumn("status_code_high_avg_2h", F.avg("status_code_high").over(w2))
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w3))
)

result = (
    metrics
    .select(
        "log_ts",
        "latency_ms",
        "status_code_high",
        "is_flagged",
        "clean_key",
        "status_code_high_max_1h",
        "status_code_high_avg_2h",
        "status_code_high_percent_rank",
    )
)
