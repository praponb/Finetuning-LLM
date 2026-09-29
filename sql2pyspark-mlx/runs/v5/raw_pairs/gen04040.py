from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("level")
    .agg(
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.avg("status_code").alias("avg_status_code_2"),
    )
    .orderBy(F.col("level").desc_nulls_last())
)
