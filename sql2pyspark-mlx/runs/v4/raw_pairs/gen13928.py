from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high").desc())
w2 = Window.partitionBy("service")
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w4 = Window.partitionBy("status_code_high_rank_2_tier").orderBy(F.col("info_avg_status_code_high").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .filter(~F.col("service").isin(["billing", "checkout", "auth"]))
    .select(
        "id",
        "service",
        "level",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

with_windows = (
    unpacked
    .withColumn("status_code_high_rank", F.rank().over(w))
    .withColumn("service_count_has_fault", F.count("has_fault").over(w2))
    .withColumn("status_code_high_rank_2", F.rank().over(w3))
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("status_code_high_rank_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_high_rank_2_tier"),
        "service",
    )
    .agg(
        F.sum("status_code_high").alias("total_status_code_high"),
        F.round(F.avg("has_fault"), 2).alias("avg_has_fault"),
        F.avg(F.when(F.col("level") == "INFO", F.col("status_code_high"))).alias("info_avg_status_code_high"),
    )
    .withColumn("info_avg_status_code_high_dense_rank", F.dense_rank().over(w4))
)

result = (
    stats
    .select(
        "status_code_high_rank_2_tier",
        "service",
        "avg_has_fault",
        "info_avg_status_code_high",
    )
    .orderBy(F.col("status_code_high_rank_2_tier").desc_nulls_last())
)
