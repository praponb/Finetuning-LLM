from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "service", "host", "level", "key", "attr_value")
    .orderBy(F.col("host").desc())
)
