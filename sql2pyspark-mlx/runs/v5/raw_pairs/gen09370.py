from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key")
w2 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("greatest_status_code_high_rank_status_code_high_bucket").desc_nulls_first(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    lines
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "service",
        "level",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

scored = (
    unpacked
    .select(
        "id",
        "service",
        "level",
        "key",
        "status_code_high",
        F.ntile(3).over(w).alias("status_code_high_bucket"),
        F.rank().over(w2).alias("status_code_high_rank"),
    )
)

base = (
    scored
    .filter(~F.col("level").isin(["ERROR", "WARN"]))
    .select(
        "id",
        "key",
        "status_code_high",
        "status_code_high_bucket",
        "status_code_high_rank",
        F.greatest("status_code_high_rank", "status_code_high_bucket").alias("greatest_status_code_high_rank_status_code_high_bucket"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select("id", "key", "status_code_high", "status_code_high_rank", "rn")
    .orderBy("id")
)
