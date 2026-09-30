from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "value",
        F.when(F.col("key").rlike("^(region|trace)$"), "GROUP_B").when(F.col("key").rlike("^(version)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy("status_code", "key_zone")
)
