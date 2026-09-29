from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select("id", "level", "status_code", "value")
    .orderBy(F.col("level").desc_nulls_last())
)
