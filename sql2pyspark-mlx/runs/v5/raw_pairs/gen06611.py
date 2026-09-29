from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode("context").alias("k", "attr_value"))
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.avg(F.when(F.col("k") == "region", F.col("latency_ms"))).alias("region_avg_latency_ms"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.sum(F.when(F.col("k") == "trace", F.col("latency_ms"))).alias("trace_total_latency_ms"),
        F.max("log_ts").alias("ended_at"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
