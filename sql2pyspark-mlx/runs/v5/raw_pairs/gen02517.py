from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        F.when(F.col("key").rlike("^(version|region)$"), "GROUP_B").when(F.col("key").rlike("^(trace)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("key_zone"),
    )
)
