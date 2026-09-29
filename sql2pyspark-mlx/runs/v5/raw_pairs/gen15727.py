from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("level", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        F.sum("latency_ms").over(w).alias("key_sum_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
    )
)

filtered = (
    metrics
    .filter(F.col("key").isNull())
    .withColumn("latency_ms_per_latency_ms_share", (F.col("latency_ms") / F.nullif("latency_ms_share", F.lit(0))))
    .withColumn("key_filled", F.coalesce("key", F.lit("DIRECT")))
)

enriched = (
    filtered
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        "key_sum_latency_ms",
        "latency_ms_cume_dist",
        "key_filled",
        F.sum("latency_ms_per_latency_ms_share").over(w4).alias("level_sum_latency_ms_per_latency_ms_share"),
    )
)

result = (
    enriched
    .groupBy("level")
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("latency_ms_per_debug"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("error_count") < 1000)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
