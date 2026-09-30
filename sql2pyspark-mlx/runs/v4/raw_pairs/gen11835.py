from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "level", "status_code", F.explode("context").alias("key", "value"))
)

result = (
    exploded
    .select(
        "id",
        "key",
        "value",
        F.when(F.col("key").rlike("^(region|version)$"), "GROUP_B").when(F.col("key").rlike("^(trace)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
