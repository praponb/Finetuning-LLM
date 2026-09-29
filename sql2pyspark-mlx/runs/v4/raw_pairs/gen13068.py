from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.count_if(F.col("k") == "region").alias("region_events"),
    )
    .orderBy(F.col("span_minutes").desc())
)
