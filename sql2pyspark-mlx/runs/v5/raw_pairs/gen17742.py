from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(
        "level",
        F.when(F.col("status_code") >= 470, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.avg(F.when(F.col("k") == "trace", F.col("status_code"))).alias("trace_avg_status_code"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .orderBy(F.col("level").desc_nulls_first())
)
