from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "k",
        "v",
        F.when(F.col("level").rlike("^(WARN|DEBUG)$"), "PRIORITY").when(F.col("level").rlike("^(INFO|ERROR)$"), "EXTENDED").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("level_zone").desc())
)
