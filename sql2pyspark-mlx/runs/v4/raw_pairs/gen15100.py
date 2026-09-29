from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("host").orderBy("latency_ms_percent_rank")
w3 = Window.partitionBy("k").orderBy("greatest_latency_ms_percent_rank_latency_ms")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "k",
        "latency_ms_dense_rank",
        "latency_ms_percent_rank",
        F.greatest("latency_ms_percent_rank", "latency_ms").alias("greatest_latency_ms_percent_rank_latency_ms"),
        F.lower("service").alias("service_lower"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.rank().over(w2).alias("latency_ms_percent_rank_rank"),
        F.rank().over(w3).alias("greatest_latency_ms_percent_rank_latency_ms_rank"),
    )
)

summary = (
    metrics
    .cube(
        F.when(F.col("latency_ms_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("latency_ms_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("greatest_latency_ms_percent_rank_latency_ms_rank", 0.95).alias("greatest_latency_ms_percent_rank_latency_ms_rank_p95"),
        F.percentile_approx("latency_ms_percent_rank_rank", 0.5).alias("latency_ms_percent_rank_rank_p50"),
        F.avg(F.when(F.col("service") == "checkout", F.col("greatest_latency_ms_percent_rank_latency_ms"))).alias("checkout_avg_greatest_latency_ms_percent_rank_latency_ms"),
    )
)

result = (
    summary
    .select(
        "latency_ms_dense_rank_tier",
        "greatest_latency_ms_percent_rank_latency_ms_rank_p95",
        "latency_ms_percent_rank_rank_p50",
    )
    .orderBy("latency_ms_dense_rank_tier")
)
