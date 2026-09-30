from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .groupBy("service", F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.count_if(F.col("level") == "INFO").alias("info_events"),
        F.max("log_ts").alias("last_seen"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
    )
    .filter(F.col("duration_min") >= 0)
    .orderBy("service")
)
