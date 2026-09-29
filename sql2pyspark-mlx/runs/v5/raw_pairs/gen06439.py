from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "key",
        F.when(F.col("key").rlike("^(trace|version)$"), "GROUP_A").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc_nulls_last())
)
