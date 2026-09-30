from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "key",
        "value",
        F.when(F.col("key").rlike("^(trace|region)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc_nulls_last(), "host")
)
