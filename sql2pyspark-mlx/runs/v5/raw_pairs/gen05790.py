from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
    )
)

result = (
    parsed
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 406, "HIGH").otherwise("LOW"))
    .cube("status_code_tier", "service")
    .agg(
        F.sum(F.when(F.col("service") == "auth", F.col("status_code"))).alias("auth_total_status_code"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("auth_rows")
)
