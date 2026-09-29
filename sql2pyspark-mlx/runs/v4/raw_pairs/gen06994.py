from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("level").rlike("^(WARN|ERROR)$"), "OTHER_ZONE").when(F.col("level").rlike("^(INFO|DEBUG)$"), "PRIORITY").otherwise("GROUP_B").alias("level_zone"),
    )
)
