from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2320, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
        "host",
    )
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.count("*").alias("event_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
)
