from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit8").desc_nulls_last())
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.orderBy(F.col("flag_bit8").asc_nulls_last())
w4 = Window.orderBy(F.col("flag_bit8_percent_rank").desc())
w5 = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w_grp = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w_hist = Window.orderBy(F.col("flag_bit8_percent_rank_cume_dist").desc())
w8 = Window.orderBy(F.col("flag_bit8_cume_dist").asc_nulls_first())
w9 = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w10 = Window.orderBy(F.col("flag_bit8_cume_dist_cume_dist").desc())
w11 = Window.orderBy(F.col("status_code_high_cume_dist").asc_nulls_first())
w12 = Window.orderBy(F.col("flag_bit8_percent_rank_cume_dist_whole").desc())
w13 = Window.orderBy(F.col("flag_bit8_cume_dist_cume_dist").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

unpacked = (
    flattened
    .filter(F.col("latency_ms") < 1328)
    .select(
        "id",
        "latency_ms",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

scored = (
    unpacked
    .filter(F.col("flag_bit8") != 1)
    .select(
        "*",
        F.percent_rank().over(w).alias("flag_bit8_percent_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "latency_ms",
        "status_code_high",
        "flag_bit8_percent_rank",
        "latency_ms_rank",
        F.cume_dist().over(w3).alias("flag_bit8_cume_dist"),
        F.cume_dist().over(w4).alias("flag_bit8_percent_rank_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w5).alias("status_code_high_cume_dist"),
        F.percent_rank().over(w_grp).alias("latency_ms_percent_rank"),
        F.cume_dist().over(w_hist).alias("flag_bit8_percent_rank_cume_dist_cume_dist"),
        F.cume_dist().over(w8).alias("flag_bit8_cume_dist_cume_dist"),
        F.rank().over(w9).alias("status_code_high_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("flag_bit8_percent_rank_cume_dist_cume_dist") > 0.79)
    .select(
        "id",
        "latency_ms",
        "status_code_high",
        "flag_bit8_percent_rank",
        "latency_ms_rank",
        "status_code_high_cume_dist",
        "latency_ms_percent_rank",
        "flag_bit8_cume_dist_cume_dist",
        "status_code_high_rank",
        F.ceil("flag_bit8_percent_rank_cume_dist").alias("flag_bit8_percent_rank_cume_dist_whole"),
    )
)

with_windows_app_logs = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w10).alias("flag_bit8_cume_dist_cume_dist_cume_dist"),
        F.dense_rank().over(w11).alias("status_code_high_cume_dist_dense_rank"),
        F.rank().over(w12).alias("flag_bit8_percent_rank_cume_dist_whole_rank"),
        F.cume_dist().over(w13).alias("flag_bit8_cume_dist_cume_dist_cume_dist_2"),
        F.cume_dist().over(w_grp).alias("latency_ms_cume_dist"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("latency_ms_cume_dist") >= 0.71, "PREMIUM").otherwise("STANDARD").alias("latency_ms_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("flag_bit8_percent_rank_cume_dist_whole_rank", 0.9).alias("flag_bit8_percent_rank_cume_dist_whole_rank_p90"),
        F.percentile_approx("flag_bit8_percent_rank_cume_dist_whole_rank", [0.25, 0.5, 0.75]).alias("flag_bit8_percent_rank_cume_dist_whole_rank_quartiles"),
        F.percentile_approx("latency_ms_percent_rank", 0.5).alias("latency_ms_percent_rank_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("latency_ms_percent_rank_p50") < 1000000)
)
