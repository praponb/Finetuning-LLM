from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    parsed
    .groupBy("host")
    .agg(
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.avg("status_code_high").alias("avg_status_code_high"),
        F.sum(F.when(F.col("host") == "h1", F.col("status_code_high")).otherwise(0)).alias("h1_status_code_high"),
    )
)
