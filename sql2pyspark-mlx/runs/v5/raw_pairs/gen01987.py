from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h4", "h3"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["trace"].alias("trace"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    unpacked
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc())
)
