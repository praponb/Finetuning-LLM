from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("is_flagged").desc_nulls_last())
w3 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.partitionBy("host", "attr_key")
w5 = Window.partitionBy("host").orderBy(F.col("is_flagged_cume_dist").desc_nulls_last())
w6 = Window.orderBy(F.col("corr_latency_ms_status_code").desc_nulls_first())
w7 = Window.partitionBy("attr_key")
w8 = Window.partitionBy("attr_key").orderBy(F.col("is_flagged_cume_dist").desc_nulls_first())
w9 = Window.orderBy(F.col("is_flagged_cume_dist_dense_rank").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "host",
        "latency_ms",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull())
    .withColumn("status_code_per_is_flagged", (F.col("status_code") / F.nullif("is_flagged", F.lit(0))))
    .withColumn("least_is_flagged_latency_ms", F.least("is_flagged", "latency_ms"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

scored = (
    filtered
    .withColumn("host_sum_status_code_per_is_flagged", F.sum("status_code_per_is_flagged").over(w))
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w2))
)

scored_app_logs = (
    scored
    .select(
        "latency_ms",
        "status_code",
        "attr_key",
        "status_code_per_is_flagged",
        "least_is_flagged_latency_ms",
        "host_sum_status_code_per_is_flagged",
        "is_flagged_cume_dist",
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
        F.corr("latency_ms", "status_code").over(w4).alias("corr_latency_ms_status_code"),
        F.cume_dist().over(w5).alias("is_flagged_cume_dist_cume_dist"),
    )
)

enriched = (
    scored_app_logs
    .select(
        "latency_ms",
        "status_code",
        "status_code_per_is_flagged",
        "host_sum_status_code_per_is_flagged",
        "latency_ms_cume_dist",
        "corr_latency_ms_status_code",
        F.cume_dist().over(w6).alias("corr_latency_ms_status_code_cume_dist"),
        F.corr("status_code", "latency_ms").over(w7).alias("corr_status_code_latency_ms"),
        F.dense_rank().over(w8).alias("is_flagged_cume_dist_dense_rank"),
    )
)

scored_app_logs_2 = (
    enriched
    .filter(F.col("status_code_per_is_flagged") >= 28)
    .select(
        "status_code",
        "status_code_per_is_flagged",
        "host_sum_status_code_per_is_flagged",
        "latency_ms_cume_dist",
        "corr_latency_ms_status_code",
        "corr_latency_ms_status_code_cume_dist",
        "is_flagged_cume_dist_dense_rank",
        F.percent_rank().over(w9).alias("is_flagged_cume_dist_dense_rank_percent_rank"),
    )
)

result = (
    scored_app_logs_2
    .cube(
        F.when(F.col("is_flagged_cume_dist_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("is_flagged_cume_dist_dense_rank_tier"),
    )
    .agg(F.count("*").alias("cnt"), F.count("*").alias("cnt_2"))
    .filter(F.col("cnt") < 1000)
)
