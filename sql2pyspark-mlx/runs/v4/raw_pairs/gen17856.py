from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("service", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running = Window.partitionBy("k").orderBy(F.col("is_flagged").desc())
w_grp = Window.partitionBy("service", "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("service")
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

parsed = (
    lines
    .filter(F.col("latency_ms") > 1349)
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .withColumn("is_flagged_avg_1h", F.avg("is_flagged").over(w))
    .withColumn("latency_ms_sum_30m", F.sum("latency_ms").over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.dense_rank().over(w_running).alias("is_flagged_dense_rank"),
        F.sum("latency_ms_sum_30m").over(w_grp).alias("latency_ms_sum_30m_sum_15m"),
        F.max("status_code_high").over(w5).alias("service_max_status_code_high"),
        F.count("is_flagged").over(w_hist).alias("is_flagged_count_15m"),
    )
)

result = (
    metrics
    .groupBy(
        "service",
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
    )
)
