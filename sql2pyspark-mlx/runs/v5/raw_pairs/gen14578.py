from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)
