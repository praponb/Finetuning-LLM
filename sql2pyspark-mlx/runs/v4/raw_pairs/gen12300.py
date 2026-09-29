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
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy("service", F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code")).otherwise(0)).alias("debug_status_code"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
    )
    .filter(F.col("status_code_p95") >= 0)
    .orderBy(F.col("debug_status_code").desc())
)
