from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("key").orderBy("status_code")
w3 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w_running = Window.orderBy(F.col("latency_ms_percent_rank").desc_nulls_last())
w5 = Window.partitionBy("host")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

base = (
    lines
    .filter(F.col("key").isNotNull())
    .withColumn("status_code_minus_latency_ms", (F.col("status_code") - F.col("latency_ms")))
    .withColumn("host_upper", F.upper("host"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("DIRECT")))
)

enriched = base.withColumn("latency_ms_percent_rank", F.percent_rank().over(w))

filtered = (
    enriched
    .filter(F.col("latency_ms").between(818, 1053))
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "status_code_minus_latency_ms",
        "latency_ms_percent_rank",
        F.round((F.col("latency_ms") / F.col("status_code")), 2).alias("latency_ms_per_status_code"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") > 323)
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "host",
        "latency_ms",
        "key",
        "latency_ms_per_status_code",
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
        F.dense_rank().over(w3).alias("status_code_dense_rank_2"),
        F.percent_rank().over(w_running).alias("latency_ms_percent_rank_percent_rank"),
        F.corr("latency_ms", "status_code").over(w5).alias("corr_latency_ms_status_code"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_code_dense_rank_2") >= 6, "HIGH").otherwise("LOW").alias("status_code_dense_rank_2_tier"),
        "key",
    )
    .agg(
        F.round(F.sum("latency_ms_percent_rank_percent_rank") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("latency_ms_percent_rank_percent_rank_per_h4"),
        F.percentile_approx("status_code_dense_rank", [0.25, 0.5, 0.75]).alias("status_code_dense_rank_quartiles"),
        F.sum("status_code_dense_rank_2").alias("total_status_code_dense_rank_2"),
        F.round(F.avg("corr_latency_ms_status_code"), 2).alias("avg_corr_latency_ms_status_code"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
)
