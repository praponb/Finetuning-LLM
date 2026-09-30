from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "EXTENDED").otherwise("CORE").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").asc_nulls_last())
)
