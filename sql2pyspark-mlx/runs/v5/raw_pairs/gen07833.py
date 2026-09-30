from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key")
w_running = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("key", "service").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.stddev_samp("latency_ms").over(w2).alias("key_stddev_samp_latency_ms"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "key",
        "attr_value",
        "seq_no",
        F.ntile(3).over(w_running).alias("status_code_bucket"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
        F.round((F.col("key_stddev_samp_latency_ms") / F.sum("key_stddev_samp_latency_ms").over(w5)), 4).alias("key_stddev_samp_latency_ms_share"),
        F.last("key_stddev_samp_latency_ms", ignorenulls=True).over(w6).alias("last_known_key_stddev_samp_latency_ms"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("key").isNotNull() & (F.col("key_stddev_samp_latency_ms_share") >= 0.31))
    .select("*", F.initcap(F.trim("service")).alias("service_title"))
)

result = (
    filtered
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.round(F.sum("key_stddev_samp_latency_ms_share") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("key_stddev_samp_latency_ms_share_per_trace"),
        F.max("log_ts").alias("ended_at"),
        F.min("log_ts").alias("first_seen"),
        F.percentile_approx("last_known_key_stddev_samp_latency_ms", [0.25, 0.5, 0.75]).alias("last_known_key_stddev_samp_latency_ms_quartiles"),
    )
    .orderBy(F.col("activity_day").desc_nulls_first())
)
