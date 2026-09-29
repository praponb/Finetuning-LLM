from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .select("id", "level", "status_code", "key", "value")
    .orderBy(F.col("level").desc_nulls_last())
)
