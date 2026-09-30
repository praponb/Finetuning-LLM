from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "key",
        F.when(F.col("level").rlike("^(INFO|ERROR)$"), "GROUP_A").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("host").desc_nulls_last(), "status_code")
)
