from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code")
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = (
    flattened
    .filter(F.col("latency_ms") == 765)
    .select(
        "*",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("k").isNotNull())
    .withColumn("status_code_times_latency_ms_percent_rank", (F.col("status_code") * F.col("latency_ms_percent_rank")))
    .withColumn("status_code_dense_rank_times_status_code", (F.col("status_code_dense_rank") * F.col("status_code")))
)

cleaned = (
    filtered
    .filter(F.col("k").isNotNull())
    .select(
        "level",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "status_code_dense_rank",
        "latency_ms_percent_rank",
        "status_code_times_latency_ms_percent_rank",
        "status_code_dense_rank_times_status_code",
        F.coalesce("status_code_times_latency_ms_percent_rank", F.lit(0)).alias("status_code_times_latency_ms_percent_rank_filled"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

daily = (
    cleaned
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 417, "HIGH").otherwise("LOW"))
    .withColumn("latency_ms_percent_rank_tier", F.when(F.col("latency_ms_percent_rank") >= 0.76, "PREMIUM").otherwise("STANDARD"))
    .withColumn("status_code_times_latency_ms_percent_rank_tier", F.when(F.col("status_code_times_latency_ms_percent_rank") >= 77, "LARGE").otherwise("SMALL"))
    .rollup(
        "status_code_tier",
        "latency_ms_percent_rank_tier",
        "status_code_times_latency_ms_percent_rank_tier",
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.countDistinct("level").alias("distinct_levels"),
        F.grouping("status_code_tier").alias("grouping_id"),
    )
)

result = (
    daily
    .select(
        "status_code_tier",
        "latency_ms_percent_rank_tier",
        "status_code_times_latency_ms_percent_rank_tier",
        "latency_ms_bit_and",
        "error_count",
        "distinct_levels",
        "grouping_id",
    )
    .orderBy(F.col("error_count").desc_nulls_first(), F.col("distinct_levels").desc())
)
