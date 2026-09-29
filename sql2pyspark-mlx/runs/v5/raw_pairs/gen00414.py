from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("key") == "version", 1)), F.lit(0)), 2).alias("status_code_per_version"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_count"),
    )
    .filter(F.col("version_count") < 1000)
    .orderBy(F.col("version_count").desc_nulls_first())
)
