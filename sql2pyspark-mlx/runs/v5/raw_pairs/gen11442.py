from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code") <= 319)
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "payload_json",
        F.size("context").alias("num_context"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .groupBy("level", "host")
    .agg(
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code")).otherwise(0)).alias("debug_status_code"),
        F.min("status_code").alias("min_status_code"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
    .filter(F.col("status_code_bit_or") > 1)
)
