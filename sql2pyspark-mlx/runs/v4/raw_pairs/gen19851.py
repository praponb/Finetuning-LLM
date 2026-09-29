from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select("id", "service", "log_ts", "level", "attr_key", "value")
    .orderBy(F.col("attr_key").desc())
)
