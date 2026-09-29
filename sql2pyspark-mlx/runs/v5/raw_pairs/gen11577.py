from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "log_ts", "attr_key")
    .orderBy(F.col("service").asc_nulls_last(), F.col("log_ts").desc())
)
