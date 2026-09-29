from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.min("log_ts").alias("first_seen"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
)
