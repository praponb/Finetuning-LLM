from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    exploded
    .filter(F.col("latency_ms").between(608, 2174))
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

filtered = (
    unpacked
    .filter(F.col("key").isNotNull() & (F.col("status_code_high") <= 2))
    .select(
        "*",
        F.initcap(F.trim("key")).alias("key_title"),
        (F.col("status_code") + F.col("latency_ms")).alias("status_code_plus_latency_ms"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_plus_latency_ms") >= 83, "HIGH").otherwise("LOW").alias("status_code_plus_latency_ms_tier"),
        F.when(F.col("status_code") >= 418, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("key")).alias("key_list_2"),
    )
    .orderBy(F.col("status_code_tier").asc_nulls_first())
)
