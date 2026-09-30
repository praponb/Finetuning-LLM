from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("has_fault").desc_nulls_last(), "id", "k")

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
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "host",
        "status_code",
        "k",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

deduped = (
    flattened_attrs
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "k", "v", "status_code_high", "has_fault")
)

cleaned = (
    deduped
    .filter(F.col("k").isNotNull() & (F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "k",
        "v",
        "status_code_high",
        "has_fault",
        F.col("has_fault").cast("string").alias("has_fault_num"),
        F.round((F.col("has_fault") * 0.85), 2).alias("has_fault_scaled"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.when(F.col("has_fault") >= 1, "LARGE").otherwise("SMALL").alias("has_fault_tier"),
    )
    .agg(
        F.percentile_approx("has_fault", 0.95).alias("has_fault_p95"),
        F.percentile_approx("has_fault_scaled", [0.25, 0.5, 0.75]).alias("has_fault_scaled_quartiles"),
    )
    .filter(F.col("has_fault_p95") > 0)
)

result = totals.select("has_fault_tier", "has_fault_scaled_quartiles")
