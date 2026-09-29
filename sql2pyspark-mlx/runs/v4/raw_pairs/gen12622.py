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
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select("service", "host", "level", "status_code", "k")
    .orderBy(F.col("host").desc(), "k")
)
