from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.max(F.when(F.col("service") == "search", F.col("status_code"))).alias("search_max_status_code"),
        F.min("log_ts").alias("first_log_ts"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.avg("status_code").alias("avg_status_code"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("status_code_per_search"),
    )
    .orderBy(F.col("search_max_status_code").desc())
)
