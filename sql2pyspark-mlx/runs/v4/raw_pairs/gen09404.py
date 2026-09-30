from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "CORE").otherwise("GROUP_A").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").asc_nulls_first())
)
