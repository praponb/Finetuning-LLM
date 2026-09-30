from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .groupingSets([["hour_window", "host"], ["hour_window"]], "hour_window", "host")
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .filter(F.col("latency_ms_stddev") >= 0)
    .orderBy(F.col("latency_ms_stddev").desc())
)
