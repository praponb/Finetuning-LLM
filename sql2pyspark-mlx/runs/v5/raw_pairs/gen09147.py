from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        "key",
        F.when(F.col("status_code") >= 455, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code")).otherwise(0)).alias("info_status_code"),
    )
    .orderBy(F.col("key").desc_nulls_last())
)
