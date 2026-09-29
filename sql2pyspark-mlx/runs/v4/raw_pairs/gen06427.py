from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2126, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or_2"),
    )
    .filter(F.col("search_count") >= 1)
)
