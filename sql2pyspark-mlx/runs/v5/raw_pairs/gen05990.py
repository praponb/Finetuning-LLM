from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key")
w_recent = Window.partitionBy("service")
w5 = Window.partitionBy("level")
w6 = Window.partitionBy("key", "service")
w7 = Window.partitionBy("level").orderBy("status_code_plus_latency_ms_cume_dist")
w8 = Window.partitionBy("level").orderBy(F.col("status_code_plus_latency_ms_cume_dist_percent_rank").asc_nulls_first())
w9 = Window.partitionBy("service").orderBy(F.col("latency_ms_share").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
    )
)

valid = (
    enriched
    .filter(F.col("status_code_cume_dist") < 0.34)
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        "latency_ms_cume_dist",
        "status_code_cume_dist",
        (F.col("status_code") + F.col("latency_ms_cume_dist")).alias("status_code_plus_latency_ms_cume_dist"),
    )
)

filtered = (
    valid
    .filter(F.col("status_code") >= 352)
    .withColumn("service_length", F.length("service"))
    .withColumn("level_prefix", F.substring("level", 1, 4))
)

with_windows = (
    filtered
    .withColumn("service_count_latency_ms_cume_dist", F.count("latency_ms_cume_dist").over(w_recent))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w5)), 4))
    .withColumn("key_count_status_code_cume_dist", F.count("status_code_cume_dist").over(w6))
    .withColumn("status_code_plus_latency_ms_cume_dist_percent_rank", F.percent_rank().over(w7))
    .withColumn("key_min_status_code_plus_latency_ms_cume_dist", F.min("status_code_plus_latency_ms_cume_dist").over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("service_count_latency_ms_cume_dist").between(9, 16))
    .select(
        "*",
        (F.col("status_code") * F.col("status_code_cume_dist")).alias("status_code_times_status_code_cume_dist"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

scored = (
    cleaned
    .select(
        "service",
        "level",
        "latency_ms",
        "value",
        "status_code_cume_dist",
        "service_length",
        "service_count_latency_ms_cume_dist",
        "latency_ms_share",
        "key_count_status_code_cume_dist",
        "status_code_plus_latency_ms_cume_dist_percent_rank",
        "key_min_status_code_plus_latency_ms_cume_dist",
        "status_code_times_status_code_cume_dist",
        F.cume_dist().over(w8).alias("status_code_plus_latency_ms_cume_dist_percent_rank_cume_dist"),
        F.corr("status_code", "latency_ms").over(w5).alias("corr_status_code_latency_ms"),
        F.rank().over(w9).alias("latency_ms_share_rank"),
    )
)

result = (
    scored
    .select(
        "service",
        "level",
        "latency_ms",
        "value",
        "status_code_cume_dist",
        "service_count_latency_ms_cume_dist",
        "latency_ms_share",
        "key_count_status_code_cume_dist",
        "status_code_plus_latency_ms_cume_dist_percent_rank",
    )
    .orderBy(F.col("latency_ms_share").asc_nulls_last(), "status_code_cume_dist")
)
