from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .groupBy("k")
    .agg(
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code"))).alias("version_total_status_code"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_rows"),
        F.max(F.when(F.col("host") == "h4", F.col("status_code"))).alias("h4_max_status_code"),
    )
    .filter(F.col("info_rows") > 1)
)
