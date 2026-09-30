from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("latency_ms") >= 2359)
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy("host", "level")
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and_2"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
    )
    .orderBy(F.col("h2_events").asc_nulls_last())
)
