from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("level", "service").orderBy(F.col("status_code").desc())
w5 = Window.orderBy(F.col("status_code_rank_times_status_code_max_15m").asc_nulls_first())
w6 = Window.orderBy(F.col("status_code_rank_times_status_code_max_15m").asc_nulls_last())
w7 = Window.orderBy("status_code_max_15m")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "level",
        "key",
        F.avg("status_code").over(w).alias("status_code_avg_2h"),
        F.max("status_code").over(w2).alias("status_code_max_15m"),
        F.sum("status_code").over(w3).alias("key_sum_status_code"),
        F.rank().over(w4).alias("status_code_rank"),
    )
)

base = (
    metrics
    .filter(F.col("status_code_avg_2h") > 378)
    .select(
        "id",
        "status_code_max_15m",
        "key_sum_status_code",
        "status_code_rank",
        F.trim("key").alias("clean_key"),
        F.floor("status_code_avg_2h").alias("status_code_avg_2h_whole"),
    )
)

filtered = (
    base
    .filter(F.col("status_code_avg_2h_whole").between(60, 74))
    .select(
        "id",
        "status_code_max_15m",
        "key_sum_status_code",
        "clean_key",
        (F.col("status_code_rank") * F.col("status_code_max_15m")).alias("status_code_rank_times_status_code_max_15m"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "status_code_max_15m",
        "status_code_rank_times_status_code_max_15m",
        F.rank().over(w5).alias("status_code_rank_times_status_code_max_15m_rank"),
        F.dense_rank().over(w6).alias("status_code_rank_times_status_code_max_15m_dense_rank"),
        F.percent_rank().over(w7).alias("status_code_max_15m_percent_rank"),
    )
)

result = (
    enriched
    .withColumn("status_code_max_15m_percent_rank_tier", F.when(F.col("status_code_max_15m_percent_rank") >= 0.64, "HIGH").otherwise("LOW"))
    .withColumn("status_code_max_15m_tier", F.when(F.col("status_code_max_15m") >= 435, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status_code_max_15m_percent_rank_tier", "status_code_max_15m_tier"], ["status_code_max_15m_percent_rank_tier"], []],
        "status_code_max_15m_percent_rank_tier",
        "status_code_max_15m_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("status_code_max_15m_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_max_15m_percent_rank_quartiles"),
        F.round(F.avg("status_code_rank_times_status_code_max_15m"), 2).alias("avg_status_code_rank_times_status_code_max_15m"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("avg_status_code_rank_times_status_code_max_15m").desc_nulls_first())
)
