from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["trace"].alias("trace"),
    )
)
