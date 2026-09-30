from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("service").orderBy(F.col("days_since_log_ts").asc_nulls_first())
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("key", "host").orderBy(F.col("days_since_log_ts").asc_nulls_first())
w5 = Window.partitionBy("key", "host")
w6 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy("key")
w_running_2 = Window.partitionBy("service").orderBy(F.col("days_since_log_ts_max_30m").asc_nulls_first())
w_running_3 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

filtered = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("level").isin(["ERROR", "DEBUG", "WARN"]))
    .select("*", F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"))
)

with_windows = (
    filtered
    .withColumn("host_max_latency_ms", F.max("latency_ms").over(w))
    .withColumn("days_since_log_ts_cume_dist", F.cume_dist().over(w2))
    .withColumn("days_since_log_ts_count_2h", F.count("days_since_log_ts").over(w3))
)

metrics = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w4).alias("days_since_log_ts_dense_rank"),
        F.count("days_since_log_ts").over(w).alias("host_count_days_since_log_ts"),
        F.round((F.col("days_since_log_ts_count_2h") / F.sum("days_since_log_ts_count_2h").over(w5)), 4).alias("days_since_log_ts_count_2h_share"),
    )
)

scored = (
    metrics
    .withColumn("days_since_log_ts_max_30m", F.max("days_since_log_ts").over(w6))
    .withColumn("key_avg_days_since_log_ts_count_2h_share", F.avg("days_since_log_ts_count_2h_share").over(w7))
)

kept_keys = (
    scored
    .select("level", "service")
    .subtract(
        df_app_logs
        .filter(F.col("service") != "auth")
        .select("level", "service")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .withColumn("days_since_log_ts_count_2h_band", F.when(F.col("days_since_log_ts_count_2h") >= 16, "High").when(F.col("days_since_log_ts_count_2h") >= 3, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .withColumn("days_since_log_ts_cume_dist_sum_30m", F.sum("days_since_log_ts_cume_dist").over(w6))
    .withColumn("days_since_log_ts_max_30m_percent_rank", F.percent_rank().over(w_running_2))
    .withColumn("service_avg_latency_ms", F.avg("latency_ms").over(w_running_3))
)
