from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    exploded
    .select("id", "log_ts", "status_code", "key", "value")
    .orderBy(F.col("log_ts").desc(), F.col("status_code").desc())
)
