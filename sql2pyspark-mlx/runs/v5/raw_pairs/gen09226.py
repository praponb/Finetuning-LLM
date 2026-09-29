from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("has_fault_count_1h").desc_nulls_first())
w3 = Window.orderBy("status_code_high_times_has_fault")
w4 = Window.orderBy(F.col("status_code_high_times_has_fault").asc_nulls_last())
w5 = Window.orderBy(F.col("has_fault_count_1h").desc_nulls_last())
w6 = Window.orderBy(F.col("status_code_high_times_has_fault_dense_rank").desc())
w7 = Window.orderBy(F.col("has_fault").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    exploded
    .filter(F.col("log_ts") < "2024-04-01")
    .select(
        "log_ts",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .select(
        "has_fault",
        "status_code_high",
        F.count("has_fault").over(w).alias("has_fault_count_1h"),
    )
)

valid = (
    with_windows
    .filter(F.col("has_fault") > 1)
    .withColumn("status_code_high_times_has_fault", (F.col("status_code_high") * F.col("has_fault")))
)

metrics = (
    valid
    .filter(F.col("has_fault_count_1h").between(6, 12))
    .withColumn("has_fault_count_1h_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_code_high_times_has_fault_percent_rank", F.percent_rank().over(w3))
    .withColumn("status_code_high_times_has_fault_dense_rank", F.dense_rank().over(w4))
)

enriched = (
    metrics
    .select(
        "has_fault",
        "has_fault_count_1h_cume_dist",
        "status_code_high_times_has_fault_percent_rank",
        "status_code_high_times_has_fault_dense_rank",
        F.rank().over(w5).alias("has_fault_count_1h_rank"),
        F.dense_rank().over(w6).alias("status_code_high_times_has_fault_dense_rank_dense_rank"),
        F.rank().over(w7).alias("has_fault_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("has_fault") > 0)
    .select(
        "has_fault_count_1h_cume_dist",
        "status_code_high_times_has_fault_percent_rank",
        "status_code_high_times_has_fault_dense_rank",
        "has_fault_count_1h_rank",
        F.round("has_fault_rank", 2).alias("has_fault_rank_rounded"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_high_times_has_fault_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("status_code_high_times_has_fault_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("status_code_high_times_has_fault_dense_rank"), 2).alias("avg_status_code_high_times_has_fault_dense_rank"),
        F.percentile_approx("status_code_high_times_has_fault_percent_rank", 0.5).alias("status_code_high_times_has_fault_percent_rank_p50"),
    )
    .filter(F.col("avg_status_code_high_times_has_fault_dense_rank") > 0)
)
