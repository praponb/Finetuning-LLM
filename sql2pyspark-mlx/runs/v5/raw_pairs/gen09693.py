from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("k").orderBy(F.col("has_fault").asc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("has_fault").asc_nulls_last())
w_ordered = Window.orderBy(F.col("has_fault_minus_status_code_high").asc_nulls_last())
w5 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("log_ts_quarter").desc_nulls_first())
w7 = Window.partitionBy("k")
w_recent = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("k").isin(["region", "version"]))
    .select(
        "id",
        "log_ts",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

remaining_keys = (
    unpacked
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") > 1170)
        .select("id")
    )
)

verified = (
    unpacked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("k").isNotNull() & (F.col("log_ts") < "2024-05-19"))
    .select("*", F.quarter("log_ts").alias("log_ts_quarter"))
)

valid = (
    cleaned
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "*",
        F.to_date("log_ts").alias("log_ts_date"),
        (F.col("has_fault") - F.col("status_code_high")).alias("has_fault_minus_status_code_high"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "k",
        "status_code_high",
        "has_fault",
        "log_ts_quarter",
        F.rank().over(w).alias("status_code_high_rank"),
        F.rank().over(w2).alias("has_fault_rank"),
        F.rank().over(w3).alias("has_fault_rank_2"),
        F.dense_rank().over(w_ordered).alias("has_fault_minus_status_code_high_dense_rank"),
        F.avg("has_fault_minus_status_code_high").over(w5).alias("has_fault_minus_status_code_high_avg_30m"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "status_code_high",
        "has_fault",
        "log_ts_quarter",
        "has_fault_rank",
        "has_fault_minus_status_code_high_dense_rank",
        "has_fault_minus_status_code_high_avg_30m",
        F.rank().over(w6).alias("log_ts_quarter_rank"),
        F.round((F.col("status_code_high_rank") / F.sum("status_code_high_rank").over(w7)), 4).alias("status_code_high_rank_share"),
        F.stddev_samp("has_fault_minus_status_code_high_dense_rank").over(w7).alias("k_stddev_samp_has_fault_minus_status_code_high_dense_rank"),
        F.cume_dist().over(w_recent).alias("status_code_high_cume_dist"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("status_code_high_rank_share") >= 0.7, "LARGE").otherwise("SMALL").alias("status_code_high_rank_share_tier"),
    )
    .agg(
        F.min("status_code_high_cume_dist").alias("min_status_code_high_cume_dist"),
        F.sum("has_fault_minus_status_code_high_avg_30m").alias("total_has_fault_minus_status_code_high_avg_30m"),
        F.percentile_approx("has_fault_minus_status_code_high_avg_30m", [0.25, 0.5, 0.75]).alias("has_fault_minus_status_code_high_avg_30m_quartiles"),
    )
)

result = (
    grouped
    .select(
        "status_code_high_rank_share_tier",
        "min_status_code_high_cume_dist",
        "total_has_fault_minus_status_code_high_avg_30m",
        "has_fault_minus_status_code_high_avg_30m_quartiles",
    )
    .orderBy(
        F.col("total_has_fault_minus_status_code_high_avg_30m").asc_nulls_first(),
        "min_status_code_high_cume_dist",
    )
)
