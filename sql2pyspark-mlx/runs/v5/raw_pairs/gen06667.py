from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
    )
)
