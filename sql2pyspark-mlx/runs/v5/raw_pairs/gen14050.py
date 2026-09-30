from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "payload_json",
        F.col("context").getItem("version").alias("version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
