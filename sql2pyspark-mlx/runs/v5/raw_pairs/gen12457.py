from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id")
    .orderBy(F.col("id").desc_nulls_last())
)
