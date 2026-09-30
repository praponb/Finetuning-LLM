from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 2434, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.sum("status_code_high").alias("total_status_code_high"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code_high"))).alias("info_total_status_code_high"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
