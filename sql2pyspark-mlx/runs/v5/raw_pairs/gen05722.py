from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("status_code_per_version"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("activity_day").desc())
)
