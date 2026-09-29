from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(~F.col("host").isin(["h1", "h2"]))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
