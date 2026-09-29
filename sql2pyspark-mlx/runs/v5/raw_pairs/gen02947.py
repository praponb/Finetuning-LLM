from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .rollup(
        "key",
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 438, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.count("*").alias("cnt"),
        F.min("log_ts").alias("first_log_ts"),
        F.count("*").alias("row_count"),
    )
)
