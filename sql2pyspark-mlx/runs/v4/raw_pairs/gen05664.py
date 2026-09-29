from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

cleaned = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull() & F.hour("log_ts").between(9, 14))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.round((F.col("latency_ms") * 1.15), 2).alias("adjusted_latency_ms"),
        F.date_format("log_ts", "yyyy-MM").alias("log_ts_label"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("log_ts") > F.lit("2025-06-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "latency_ms",
        "log_ts_label",
        "k_filled",
        F.col("adjusted_latency_ms").cast("int").alias("adjusted_latency_ms_int"),
    )
)

daily = (
    filtered
    .withColumn("adjusted_latency_ms_int_tier", F.when(F.col("adjusted_latency_ms_int") >= 64, "HIGH").otherwise("LOW"))
    .cube("adjusted_latency_ms_int_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.grouping("adjusted_latency_ms_int_tier").alias("grouping_level"),
    )
)

result = (
    daily
    .select(
        "adjusted_latency_ms_int_tier",
        "distinct_ids_2",
        "distinct_ids_3",
        "grouping_level",
    )
)
