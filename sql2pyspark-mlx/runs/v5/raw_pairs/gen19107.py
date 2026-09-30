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
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .rollup("host", F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.sum(F.when(F.col("attr_key") == "trace", F.col("status_code")).otherwise(0)).alias("trace_status_code"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles_2"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .orderBy("trace_status_code")
)
