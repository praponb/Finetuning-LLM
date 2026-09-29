from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 2001, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or_2"),
        F.count("*").alias("event_count"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
    )
)
