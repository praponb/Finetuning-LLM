from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.when(F.col("level").rlike("^(ERROR|WARN)$"), "EXTENDED").when(F.col("level").rlike("^(INFO|DEBUG)$"), "CORE").otherwise("GROUP_B").alias("level_zone"),
    )
    .orderBy(F.col("latency_ms").desc())
)
