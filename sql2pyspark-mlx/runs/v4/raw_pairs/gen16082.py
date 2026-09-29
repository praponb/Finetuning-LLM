from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)
