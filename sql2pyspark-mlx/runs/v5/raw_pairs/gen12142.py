from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("status_code_rounded").desc_nulls_last())
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("level", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_hist = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_hist_2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w9 = Window.partitionBy("service").orderBy(F.col("status_code_share").asc_nulls_first())
w10 = Window.orderBy(F.col("status_code_avg_30m").asc_nulls_first())
w11 = Window.orderBy(F.col("status_code_avg_30m_max_15m").asc_nulls_last())
w12 = Window.partitionBy("service").orderBy(F.col("status_code_rounded").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

with_windows = (
    flattened
    .filter(~F.col("level").isin(["WARN", "DEBUG"]))
    .withColumn("status_code_avg_30m", F.avg("status_code").over(w))
    .withColumn("level_avg_status_code", F.avg("status_code").over(w2))
)

valid = (
    with_windows
    .filter((F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp")) & (F.col("log_ts") >= "2023-07-15"))
    .select(
        "service",
        "log_ts",
        "level",
        "status_code",
        "status_code_avg_30m",
        "level_avg_status_code",
        F.round("status_code", 1).alias("status_code_rounded"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.max("status_code_avg_30m").over(w3).alias("status_code_avg_30m_max_15m"),
        F.dense_rank().over(w4).alias("status_code_rounded_dense_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w5)), 4).alias("status_code_share"),
        F.count("status_code_avg_30m").over(w6).alias("status_code_avg_30m_count_2h"),
    )
)

enriched = (
    metrics
    .withColumn("service_max_status_code_rounded", F.max("status_code_rounded").over(w5))
    .withColumn("status_code_avg_30m_count_2h_share", F.round((F.col("status_code_avg_30m_count_2h") / F.sum("status_code_avg_30m_count_2h").over(w2)), 4))
    .withColumn("status_code_avg_30m_count_2h_avg_30m", F.avg("status_code_avg_30m_count_2h").over(w_hist))
    .withColumn("status_code_avg_30m_avg_1h", F.avg("status_code_avg_30m").over(w_hist_2))
)

metrics_app_logs = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w9).alias("status_code_share_percent_rank"),
        F.cume_dist().over(w10).alias("status_code_avg_30m_cume_dist"),
        F.dense_rank().over(w11).alias("status_code_avg_30m_max_15m_dense_rank"),
        F.count("status_code_rounded").over(w2).alias("level_count_status_code_rounded"),
        F.dense_rank().over(w12).alias("status_code_rounded_dense_rank_2"),
    )
)

result = (
    metrics_app_logs
    .groupBy("level")
    .pivot("service", ["billing", "auth"])
    .agg(
        F.avg("level_count_status_code_rounded").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
