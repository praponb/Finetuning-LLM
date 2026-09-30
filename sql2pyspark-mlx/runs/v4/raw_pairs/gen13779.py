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
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "level",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(region|version)$"), "EXTENDED").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").asc_nulls_last())
)
