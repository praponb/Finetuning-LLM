from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode("context").alias("attr_key", "v"))
)

result = (
    exploded
    .select(
        "id",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("attr_key_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
