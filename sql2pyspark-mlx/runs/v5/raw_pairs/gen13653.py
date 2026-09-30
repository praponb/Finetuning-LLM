from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("host") == "h3", F.col("latency_ms")).otherwise(0)).alias("h3_latency_ms"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("latency_ms_bit_and").desc_nulls_last())
)
