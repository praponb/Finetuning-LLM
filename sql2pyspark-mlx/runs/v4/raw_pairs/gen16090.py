from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2446, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["latency_ms_tier", "k", "level"], ["latency_ms_tier", "k"], []],
        "latency_ms_tier",
        "k",
        "level",
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("latency_ms_per_h3"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("latency_ms")).otherwise(0)).alias("debug_latency_ms"),
        F.grouping("latency_ms_tier").alias("grouping_level"),
    )
    .orderBy("k")
)
