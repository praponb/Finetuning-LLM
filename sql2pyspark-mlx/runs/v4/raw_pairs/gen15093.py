from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)
