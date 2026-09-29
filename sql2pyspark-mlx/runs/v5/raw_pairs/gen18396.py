from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .groupBy("host")
    .agg(
        F.max(F.when(F.col("service") == "search", F.col("latency_ms"))).alias("search_max_latency_ms"),
        F.sum(F.when(F.col("host") == "h3", F.col("latency_ms")).otherwise(0)).alias("h3_latency_ms"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms")).otherwise(0)).alias("checkout_latency_ms"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
    )
    .orderBy(F.col("trace_count").desc())
)
