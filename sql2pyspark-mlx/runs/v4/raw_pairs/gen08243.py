from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_whole").desc())
w2 = Window.partitionBy("level")
w_recent = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("k")
w5 = Window.partitionBy("k", "level")
w6 = Window.partitionBy("level", "k").orderBy(F.col("latency_ms").desc_nulls_first())
w7 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.ceil("status_code").alias("status_code_whole"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("status_code_whole_dense_rank"),
        F.max("status_code_whole").over(w2).alias("level_max_status_code_whole"),
        F.rank().over(w_recent).alias("status_code_rank"),
    )
)

with_windows = (
    scored
    .withColumn("status_code_rank_share", F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w4)), 4))
    .withColumn("k_sum_latency_ms", F.sum("latency_ms").over(w4))
    .withColumn("level_max_level_max_status_code_whole", F.max("level_max_status_code_whole").over(w2))
    .withColumn("level_min_status_code_whole", F.min("status_code_whole").over(w2))
    .withColumn("k_stddev_samp_level_max_status_code_whole", F.stddev_samp("level_max_status_code_whole").over(w5))
)

with_windows_app_logs = (
    with_windows
    .withColumn("k_max_k_sum_latency_ms", F.max("k_sum_latency_ms").over(w4))
    .withColumn("latency_ms_rank", F.rank().over(w6))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w7))
    .withColumn("k_min_level_min_status_code_whole", F.min("level_min_status_code_whole").over(w4))
)

remaining_keys = (
    with_windows_app_logs
    .select("id", "level")
    .intersect(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2025-07-20")
        .select("id", "level")
    )
)

kept_rows = (
    with_windows_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy(
        F.when(F.col("level_min_status_code_whole") >= 75, "LARGE").otherwise("SMALL").alias("level_min_status_code_whole_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("level").alias("distinct_levels"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.bit_or("status_code").alias("status_code_bit_or"),
    )
)

result = (
    summary
    .select("level_min_status_code_whole_tier", "cnt", "distinct_levels", "k_list")
    .orderBy(F.col("cnt").desc(), "level_min_status_code_whole_tier")
)
