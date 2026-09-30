from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .groupBy(
        "level",
        F.when(F.col("status_code") >= 462, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.sum(F.when(F.col("service") == "billing", F.col("status_code")).otherwise(0)).alias("billing_status_code"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
    )
    .filter(F.col("version_rows") < 1)
    .orderBy(F.col("version_rows").desc_nulls_first())
)
