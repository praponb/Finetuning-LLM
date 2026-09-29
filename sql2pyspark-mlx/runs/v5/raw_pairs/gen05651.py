from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("service", "key").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("host", "service")
w5 = Window.orderBy(F.col("status_code_sum_2h").desc_nulls_first())
w6 = Window.partitionBy("key")
w_hist = Window.orderBy(F.col("status_code_sum_2h").desc_nulls_last())
w8 = Window.orderBy("status_code_sum_2h")
w9 = Window.partitionBy("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_tier").orderBy(F.col("total_status_code_sum_2h").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    lines
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "key",
        "attr_value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.sum("status_code").over(w2).alias("status_code_sum_2h"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share_2"),
    )
)

enriched = (
    scored
    .select(
        "attr_value",
        "status_code_share",
        "status_code_sum_2h",
        F.percent_rank().over(w5).alias("status_code_sum_2h_percent_rank"),
        F.count("status_code_share_2").over(w6).alias("key_count_status_code_share_2"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.rank().over(w_hist).alias("status_code_sum_2h_rank"),
        F.percent_rank().over(w8).alias("status_code_sum_2h_percent_rank_2"),
        F.rank().over(w5).alias("status_code_sum_2h_rank_2"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("status_code_sum_2h_rank") >= 4)
    .select(
        "attr_value",
        "status_code_share",
        "status_code_sum_2h",
        "status_code_sum_2h_percent_rank",
        "key_count_status_code_share_2",
        "status_code_sum_2h_percent_rank_2",
        "status_code_sum_2h_rank_2",
        F.coalesce("status_code_sum_2h_rank_2", F.lit(0)).alias("status_code_sum_2h_rank_2_filled"),
        (F.col("status_code_sum_2h_rank_2") / F.nullif("status_code_sum_2h_percent_rank", F.lit(0))).alias("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank") >= 68, "PREMIUM").otherwise("STANDARD").alias("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_tier"),
        F.when(F.col("status_code_sum_2h_rank_2") >= 7, "LARGE").otherwise("SMALL").alias("status_code_sum_2h_rank_2_tier"),
    )
    .agg(
        F.percentile_approx("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_quartiles"),
        F.sum("status_code_sum_2h").alias("total_status_code_sum_2h"),
        F.round(F.avg("status_code_sum_2h"), 2).alias("avg_status_code_sum_2h"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_sum_2h_rank_2_per_status_code_sum_2h_percent_rank_quartiles_2"),
    )
    .filter(F.col("num_rows") >= 1)
    .withColumn("total_status_code_sum_2h_dense_rank", F.dense_rank().over(w9))
)
