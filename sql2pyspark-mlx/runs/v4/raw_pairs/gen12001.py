from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "log_ts", "level", "k")
    .orderBy(F.col("k").desc_nulls_first(), F.col("host").desc())
)
