from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_whole").asc_nulls_last())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.orderBy(F.col("status_code_whole_count_30m").desc_nulls_first())
w_part = Window.orderBy(F.col("status_code_whole_count_30m").desc())
w6 = Window.partitionBy("status_code_whole_sum_1h_tier").orderBy(F.col("status_code_whole_count_30m_rank_whole_p90").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "log_ts",
        "status_code",
        "value",
        F.concat_ws("-", "host", "k").alias("host_k_key"),
    )
)

cleaned = (
    filtered
    .filter(F.col("status_code").between(366, 376))
    .withColumn("status_code_whole", F.floor("status_code"))
)

metrics = (
    cleaned
    .filter(F.col("status_code_whole") > 68)
    .select(
        "log_ts",
        "value",
        "host_k_key",
        F.cume_dist().over(w).alias("status_code_whole_cume_dist"),
        F.count("status_code_whole").over(w2).alias("status_code_whole_count_30m"),
        F.sum("status_code_whole").over(w3).alias("status_code_whole_sum_1h"),
    )
)

enriched = (
    metrics
    .withColumn("status_code_whole_count_30m_cume_dist", F.cume_dist().over(w4))
    .withColumn("status_code_whole_count_30m_rank", F.rank().over(w_part))
)

valid = (
    enriched
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("status_code_whole_count_30m_rank_whole", F.ceil("status_code_whole_count_30m_rank"))
)

cleaned_app_logs = (
    valid
    .filter(F.col("status_code_whole_count_30m_cume_dist") >= 0.24)
    .select(
        "value",
        "status_code_whole_cume_dist",
        "status_code_whole_count_30m",
        "status_code_whole_sum_1h",
        "status_code_whole_count_30m_cume_dist",
        "status_code_whole_count_30m_rank",
        "status_code_whole_count_30m_rank_whole",
        F.date_format("log_ts", "yyyy-MM").alias("log_ts_label"),
    )
)

result = (
    cleaned_app_logs
    .groupBy(
        F.when(F.col("status_code_whole_sum_1h") >= 331, "LARGE").otherwise("SMALL").alias("status_code_whole_sum_1h_tier"),
        F.when(F.col("status_code_whole_count_30m") >= 17, "PREMIUM").otherwise("STANDARD").alias("status_code_whole_count_30m_tier"),
    )
    .agg(
        F.percentile_approx("status_code_whole_count_30m_rank_whole", 0.9).alias("status_code_whole_count_30m_rank_whole_p90"),
        F.count("*").alias("num_rows"),
        F.min("status_code_whole_count_30m_rank").alias("min_status_code_whole_count_30m_rank"),
        F.count("*").alias("row_count"),
        F.percentile_approx("status_code_whole_count_30m_rank_whole", 0.95).alias("status_code_whole_count_30m_rank_whole_p95"),
        F.percentile_approx("status_code_whole_count_30m_rank", 0.9).alias("status_code_whole_count_30m_rank_p90"),
    )
    .withColumn("status_code_whole_count_30m_rank_whole_p90_percent_rank", F.percent_rank().over(w6))
)
