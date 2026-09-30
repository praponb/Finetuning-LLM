from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") >= "2023-04-13")
    .select(
        "id",
        "log_ts",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)
