from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select("service", "status_code", "value")
    .orderBy(F.col("status_code").desc_nulls_first())
)
