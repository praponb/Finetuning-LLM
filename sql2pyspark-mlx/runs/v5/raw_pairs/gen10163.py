from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    exploded
    .filter(F.col("host").isin(["h1", "h2", "h4"]))
    .select(
        "id",
        "host",
        "level",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

cleaned = (
    unpacked
    .filter(F.col("status_code_high") == 1)
    .select(
        "id",
        "host",
        "level",
        "status_code_high",
        "is_flagged",
        F.coalesce("status_code_high", F.lit(0)).alias("status_code_high_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("is_flagged") < 1)
    .select(
        "host",
        "level",
        "status_code_high",
        "is_flagged",
        "status_code_high_filled",
        F.round("status_code_high_filled", 1).alias("status_code_high_filled_rounded"),
        (F.col("is_flagged") - F.col("status_code_high")).alias("is_flagged_minus_status_code_high"),
    )
)

enriched = (
    base
    .select(
        "host",
        "status_code_high_filled_rounded",
        F.dense_rank().over(w).alias("is_flagged_dense_rank"),
        F.percent_rank().over(w).alias("is_flagged_percent_rank"),
    )
)

result = enriched.select("is_flagged_dense_rank", "is_flagged_percent_rank")
