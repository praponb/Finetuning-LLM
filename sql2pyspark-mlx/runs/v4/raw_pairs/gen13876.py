from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("key")
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms")).otherwise(0)).alias("h1_latency_ms"),
    )
)
