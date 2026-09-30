from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "key",
        F.when(F.col("key").rlike("^(region|version)$"), "GROUP_B").otherwise("CORE").alias("key_zone"),
    )
)
