from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max("log_ts").alias("ended_at"),
        F.max("log_ts").alias("last_log_ts"),
    )
    .orderBy(F.col("activity_day").asc_nulls_first())
)
