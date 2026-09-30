from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("key", "service")
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_recent = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.partitionBy("key")
w9 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_part = Window.orderBy("status_code_share_2_sum_30m")
w11 = Window.partitionBy("key").orderBy("status_code_sum_1h")
w12 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.sum("status_code").over(w3).alias("status_code_sum_1h"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
    )
)

filtered = (
    scored
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("key").isin(["trace", "region"]), "Core").otherwise("Other").alias("key_group"),
        F.floor("status_code_share_2").alias("status_code_share_2_whole"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.count("status_code_sum_1h").over(w2).alias("host_count_status_code_sum_1h"),
        F.avg("status_code_sum_1h").over(w_running).alias("status_code_sum_1h_avg_15m"),
        F.avg("status_code_share").over(w_recent).alias("status_code_share_avg_2h"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("status_code_share_2").over(w_hist).alias("status_code_share_2_sum_30m"),
        F.max("status_code_sum_1h").over(w2).alias("host_max_status_code_sum_1h"),
    )
)

remaining_keys = (
    with_windows
    .select("host")
    .intersect(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("host")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.avg("status_code_share_avg_2h").over(w8).alias("key_avg_status_code_share_avg_2h"),
        F.avg("status_code_share_2_sum_30m").over(w9).alias("status_code_share_2_sum_30m_avg_2h"),
        F.dense_rank().over(w_part).alias("status_code_share_2_sum_30m_dense_rank"),
        F.dense_rank().over(w11).alias("status_code_sum_1h_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.min("status_code_share_avg_2h").over(w2).alias("host_min_status_code_share_avg_2h"),
        F.sum("key_avg_status_code_share_avg_2h").over(w12).alias("key_avg_status_code_share_avg_2h_sum_15m"),
    )
)
