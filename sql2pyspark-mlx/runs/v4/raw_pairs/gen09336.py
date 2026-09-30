from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "level", F.explode("context").alias("attr_key", "attr_value"))
)

result = (
    flattened
    .select(
        "id",
        "level",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key").rlike("^(trace|version)$"), "OTHER_ZONE").when(F.col("attr_key").rlike("^(region)$"), "EXTENDED").otherwise("PRIORITY").alias("attr_key_zone"),
    )
)
