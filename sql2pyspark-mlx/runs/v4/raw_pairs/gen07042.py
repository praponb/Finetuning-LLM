from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("status_code") >= 396, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.max("log_ts").alias("last_seen"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("status_code_per_billing"),
        F.avg("status_code").alias("avg_status_code_2"),
        F.sum(F.when(F.col("service") == "billing", F.col("status_code")).otherwise(0)).alias("billing_status_code"),
    )
    .orderBy(F.col("status_code_tier").desc_nulls_last())
)
