from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy("status_code")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("status_code_cume_dist").desc_nulls_last())
w5 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w6 = Window.partitionBy("key", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.partitionBy("key")
w9 = Window.orderBy(F.col("latency_ms_count_1h").desc())
w10 = Window.partitionBy("level").orderBy(F.col("latency_ms_count_1h").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.count("latency_ms").over(w3).alias("latency_ms_count_1h"),
        F.min("status_code_cume_dist").over(w).alias("level_min_status_code_cume_dist"),
        F.rank().over(w4).alias("status_code_cume_dist_rank"),
        F.dense_rank().over(w5).alias("latency_ms_dense_rank"),
        F.sum("status_code_cume_dist").over(w6).alias("status_code_cume_dist_sum_2h"),
    )
)

metrics_app_logs = (
    metrics
    .withColumn("status_code_cume_dist_sum_2h_count_1h", F.count("status_code_cume_dist_sum_2h").over(w7))
    .withColumn("latency_ms_count_1h_share", F.round((F.col("latency_ms_count_1h") / F.sum("latency_ms_count_1h").over(w8)), 4))
    .withColumn("latency_ms_count_1h_cume_dist", F.cume_dist().over(w9))
)

scored = (
    metrics_app_logs
    .select(
        "latency_ms",
        "key",
        "attr_value",
        "corr_status_code_latency_ms",
        "latency_ms_count_1h",
        "level_min_status_code_cume_dist",
        "status_code_cume_dist_rank",
        "latency_ms_count_1h_share",
        "latency_ms_count_1h_cume_dist",
        F.percent_rank().over(w10).alias("latency_ms_count_1h_percent_rank"),
        F.max("latency_ms_count_1h_share").over(w).alias("level_max_latency_ms_count_1h_share"),
        F.stddev_samp("latency_ms_dense_rank").over(w).alias("level_stddev_samp_latency_ms_dense_rank"),
    )
)

stats = (
    scored
    .withColumn("latency_ms_count_1h_tier", F.when(F.col("latency_ms_count_1h") >= 16, "PREMIUM").otherwise("STANDARD"))
    .withColumn("level_min_status_code_cume_dist_tier", F.when(F.col("level_min_status_code_cume_dist") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["latency_ms_count_1h_tier", "level_min_status_code_cume_dist_tier"], ["latency_ms_count_1h_tier"]],
        "latency_ms_count_1h_tier",
        "level_min_status_code_cume_dist_tier",
    )
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
)

result = (
    stats
    .select("latency_ms_count_1h_tier", "num_keys", "trace_rows")
    .orderBy("num_keys")
)
