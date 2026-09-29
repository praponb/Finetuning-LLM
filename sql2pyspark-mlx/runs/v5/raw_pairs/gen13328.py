from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "latency_ms", F.explode("context").alias("attr_key", "value"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 1936, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("attr_key") == "trace", F.col("latency_ms"))).alias("trace_total_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("latency_ms_per_version"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy(F.col("latency_ms_per_version").desc_nulls_last())
)
