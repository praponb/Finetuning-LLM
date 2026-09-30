from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select("id", "service", "v")
    .orderBy(F.col("service").desc_nulls_first())
)
