from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "value",
        F.when(F.col("host").rlike("^(h3|h1)$"), "OTHER_ZONE").when(F.col("host").rlike("^(h2|h4)$"), "PRIORITY").otherwise("GROUP_B").alias("host_zone"),
    )
)
