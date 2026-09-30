from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("level") == "ERROR")
    .select(
        "id",
        "service",
        "level",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    flattened_attrs
    .groupBy("level", "service")
    .agg(
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code_high"))).alias("debug_total_status_code_high"),
        F.avg("status_code_high").alias("avg_status_code_high"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high_2"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
    )
)
