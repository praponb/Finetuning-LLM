from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1879, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["attr_key", "latency_ms_tier"], ["attr_key"]],
        "attr_key",
        "latency_ms_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("attr_key").desc_nulls_first())
)
