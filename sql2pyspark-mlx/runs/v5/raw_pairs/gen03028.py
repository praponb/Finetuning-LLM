from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "key",
        "value",
        F.when(F.col("level").rlike("^(DEBUG|WARN)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
)
