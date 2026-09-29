from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code") >= 438, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        "host",
    )
    .agg(
        F.sum("status_code").alias("total_status_code"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles_2"),
        F.min("log_ts").alias("started_at"),
    )
    .filter(F.col("total_status_code") >= 0)
    .orderBy(F.col("total_status_code").desc_nulls_last())
)
