from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context").getItem("region").alias("region"),
    )
)
