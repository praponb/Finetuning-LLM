from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy(
        "key",
        F.when(F.col("latency_ms") >= 1890, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.sum(F.when(F.col("service") == "auth", F.col("latency_ms")).otherwise(0)).alias("auth_latency_ms"),
    )
    .orderBy("auth_latency_ms")
)
