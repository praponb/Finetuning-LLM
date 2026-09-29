from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "host").orderBy(F.col("is_flagged").asc_nulls_last())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "host")
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("host", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("host").orderBy(F.col("level_stddev_samp_status_code_high").desc_nulls_last())
w_ordered = Window.partitionBy("level").orderBy(F.col("level_sum_is_flagged").desc())
w8 = Window.partitionBy("level", "host").orderBy(F.col("status_code_high").asc_nulls_first())
w9 = Window.partitionBy("host", "level")
w10 = Window.partitionBy("level").orderBy(F.col("is_flagged_dense_rank").desc())
w11 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w12 = Window.partitionBy("level").orderBy(F.col("level_sum_is_flagged_rank").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "host",
        "log_ts",
        "level",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    flattened_attrs
    .withColumn("is_flagged_dense_rank", F.dense_rank().over(w))
    .withColumn("level_sum_is_flagged", F.sum("is_flagged").over(w2))
    .withColumn("level_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w3))
)

remaining_keys = (
    with_windows
    .select("host", "level")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") < 1676)
        .select("host", "level")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("level_sum_is_flagged_avg_30m", F.avg("level_sum_is_flagged").over(w4))
    .withColumn("is_flagged_dense_rank_avg_2h", F.avg("is_flagged_dense_rank").over(w5))
    .withColumn("level_stddev_samp_status_code_high_percent_rank", F.percent_rank().over(w6))
    .withColumn("level_sum_is_flagged_rank", F.rank().over(w_ordered))
    .withColumn("status_code_high_rank", F.rank().over(w8))
)

with_windows_app_logs = (
    scored
    .select(
        "log_ts",
        "level",
        "is_flagged",
        "level_sum_is_flagged",
        "level_sum_is_flagged_avg_30m",
        "level_stddev_samp_status_code_high_percent_rank",
        "status_code_high_rank",
        F.round((F.col("level_sum_is_flagged_rank") / F.sum("level_sum_is_flagged_rank").over(w9)), 4).alias("level_sum_is_flagged_rank_share"),
        F.percent_rank().over(w10).alias("is_flagged_dense_rank_percent_rank"),
        F.max("is_flagged_dense_rank").over(w11).alias("is_flagged_dense_rank_max_15m"),
        F.rank().over(w12).alias("level_sum_is_flagged_rank_rank"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code_high_rank")).otherwise(0)).alias("info_status_code_high_rank"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.percentile_approx("level_sum_is_flagged_avg_30m", 0.9).alias("level_sum_is_flagged_avg_30m_p90"),
        F.max("log_ts").alias("last_log_ts"),
    )
)
