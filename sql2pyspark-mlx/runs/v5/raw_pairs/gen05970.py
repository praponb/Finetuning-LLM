from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "GROUP_A").when(F.col("level").rlike("^(WARN|DEBUG)$"), "GROUP_B").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy(F.col("level").desc(), "attr_key")
)
