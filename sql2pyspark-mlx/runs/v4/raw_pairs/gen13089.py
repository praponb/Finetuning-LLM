from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode("context").alias("key", "value"))
)

result = (
    exploded
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("key") == "region", F.col("latency_ms"))).alias("region_total_latency_ms"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms"))).alias("checkout_total_latency_ms"),
    )
    .orderBy(F.col("region_total_latency_ms").desc_nulls_first())
)
