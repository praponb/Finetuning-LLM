from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("key", "host")
w3 = Window.partitionBy("key")
w_ordered = Window.orderBy("is_version")
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.orderBy("key_min_status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select("*", F.when(F.col("key") == "version", 1).otherwise(0).alias("is_version"))
)

metrics = (
    base
    .filter(F.hour("log_ts").between(9, 12))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "is_version",
        F.sum("status_code").over(w_recent).alias("status_code_sum_2h"),
        F.avg("status_code").over(w2).alias("key_avg_status_code"),
        F.min("status_code").over(w3).alias("key_min_status_code"),
        F.percent_rank().over(w_ordered).alias("is_version_percent_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.count("key_min_status_code").over(w5).alias("key_min_status_code_count_2h"),
        F.rank().over(w6).alias("key_min_status_code_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("is_version_percent_rank") >= 0.72, "LARGE").otherwise("SMALL").alias("is_version_percent_rank_tier"),
    )
    .agg(
        F.round(F.sum("key_min_status_code_count_2h") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("key_min_status_code_count_2h_per_trace"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.round(F.sum("key_min_status_code_rank") / F.nullif(F.count(F.when(F.col("key") == "region", 1)), F.lit(0)), 2).alias("key_min_status_code_rank_per_region"),
        F.bit_or("status_code").alias("status_code_bit_or_2"),
    )
)
