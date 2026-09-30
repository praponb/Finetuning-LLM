from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "value",
        F.when(F.col("key").rlike("^(trace|version)$"), "GROUP_A").otherwise("CORE").alias("key_zone"),
    )
)
