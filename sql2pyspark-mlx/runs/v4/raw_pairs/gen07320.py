from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        F.col("context").getItem("region").alias("region"),
        F.col("context")["trace"].alias("trace"),
    )
)
