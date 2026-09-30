from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .select("id")
    .orderBy(F.col("id").asc_nulls_first())
)
