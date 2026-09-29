from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
    )
)
