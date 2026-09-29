from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("key", "value"))
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "key",
        "value",
        F.when(F.col("key").rlike("^(trace|version)$"), "CORE").when(F.col("key").rlike("^(region)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy("id")
)
