from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "context",
        F.size("context").alias("num_context"),
        F.col("context")["region"].alias("region"),
    )
)
