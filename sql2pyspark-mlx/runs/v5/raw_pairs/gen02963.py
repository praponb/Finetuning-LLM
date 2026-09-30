from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") <= 375)
    .select(
        "id",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    parsed
    .filter(F.col("status_code_high") <= 2)
    .withColumn("status_code_high_whole", F.ceil("status_code_high"))
)

kept_keys = (
    base
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "ERROR")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("has_fault") == 1)
    .select(
        "id",
        "status_code",
        "status_code_high",
        "has_fault",
        F.round("status_code_high", 1).alias("status_code_high_rounded"),
    )
)

with_windows = filtered.select("*", F.dense_rank().over(w).alias("status_code_dense_rank"))

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sum("status_code_high_rounded").alias("total_status_code_high_rounded"),
        F.percentile_approx("has_fault", [0.25, 0.5, 0.75]).alias("has_fault_quartiles"),
        F.percentile_approx("status_code_dense_rank", [0.25, 0.5, 0.75]).alias("status_code_dense_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("total_status_code_high_rounded") >= 0)
)
