from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k", "level")
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("k", "level").orderBy(F.col("log_ts_hour").desc())
w4 = Window.partitionBy("level").orderBy(F.col("k_sum_log_ts_hour").desc())
w5 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("k")
w_running = Window.partitionBy("k").orderBy(F.col("log_ts_hour_count_2h").desc_nulls_last())

flattened = (
    df_app_logs
    .select("id", "log_ts", "level", F.explode_outer("context").alias("k", "value"))
)

valid = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("log_ts") >= "2024-06-01"))
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        "value",
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("log_ts_hour").over(w_grp).alias("k_sum_log_ts_hour"),
        F.count("log_ts_hour").over(w2).alias("log_ts_hour_count_2h"),
        F.percent_rank().over(w3).alias("log_ts_hour_percent_rank"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w4).alias("k_sum_log_ts_hour_dense_rank"),
        F.count("k_sum_log_ts_hour").over(w5).alias("k_sum_log_ts_hour_count_1h"),
        F.stddev_samp("k_sum_log_ts_hour").over(w6).alias("k_stddev_samp_k_sum_log_ts_hour"),
        F.percent_rank().over(w_running).alias("log_ts_hour_count_2h_percent_rank"),
        F.min("log_ts_hour").over(w_grp).alias("k_min_log_ts_hour"),
    )
)

cleaned = (
    scored
    .filter(~F.col("level").isin(["DEBUG", "WARN", "INFO"]))
    .select(
        "id",
        "k",
        "k_sum_log_ts_hour",
        "log_ts_hour_percent_rank",
        "k_sum_log_ts_hour_dense_rank",
        "k_sum_log_ts_hour_count_1h",
        "log_ts_hour_count_2h_percent_rank",
        "k_min_log_ts_hour",
        F.col("log_ts_hour_count_2h_percent_rank").cast("int").alias("log_ts_hour_count_2h_percent_rank_int"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("k_min_log_ts_hour") >= 88, "LARGE").otherwise("SMALL").alias("k_min_log_ts_hour_tier"),
    )
    .agg(
        F.max("k_sum_log_ts_hour_dense_rank").alias("max_k_sum_log_ts_hour_dense_rank"),
        F.round(F.sum("k_sum_log_ts_hour") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("k_sum_log_ts_hour_per_region"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
