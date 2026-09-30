from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "service")
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        F.sum("latency_ms").over(w).alias("key_sum_latency_ms"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)

cleaned = (
    metrics
    .filter((F.col("key").eqNullSafe(F.lit("version"))) & (F.col("latency_ms_rank") > 6))
    .withColumn("key_title", F.initcap(F.trim("key")))
    .withColumn("key_filled", F.coalesce("key", F.lit("UNKNOWN")))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "status_code",
        "key",
        "key_sum_latency_ms",
        "latency_ms_rank",
        "key_title",
        "key_filled",
        F.floor("status_code").alias("status_code_whole"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

filtered = (
    cleaned_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.round((F.col("key_sum_latency_ms") * 1.15), 2).alias("key_sum_latency_ms_scaled"),
        F.round((F.col("latency_ms_rank") * 0.85), 2).alias("latency_ms_rank_adjusted"),
    )
)

result = (
    filtered
    .groupBy("key")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.sort_array(F.collect_set("key")).alias("key_list_2"),
    )
    .orderBy(F.col("key").desc())
)
