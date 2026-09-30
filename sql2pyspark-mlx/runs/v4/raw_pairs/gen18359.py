from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "value",
        F.when(F.col("attr_key").rlike("^(trace|region)$"), "GROUP_A").otherwise("GROUP_B").alias("attr_key_zone"),
    )
)
