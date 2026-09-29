from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("k")
w5 = Window.orderBy("created_at_hour_avg_1h")
w6 = Window.partitionBy("k").orderBy(F.col("created_at_hour_max_30m_rounded").desc_nulls_first())
w7 = Window.orderBy(F.col("created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded").desc_nulls_last())
w8 = Window.orderBy(F.col("created_at_hour_avg_1h_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded").asc_nulls_first())

exploded = df_carts.select("id", "created_at", F.explode_outer("attributes").alias("k", "v"))

cleaned = (
    exploded
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 02:00:00"))) & (~F.col("k").eqNullSafe(F.lit("device"))))
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "k",
        "created_at_hour",
        F.sum("created_at_hour").over(w).alias("created_at_hour_sum_15m"),
        F.avg("created_at_hour").over(w2).alias("created_at_hour_avg_1h"),
        F.max("created_at_hour").over(w3).alias("created_at_hour_max_30m"),
    )
)

valid = (
    scored
    .filter(F.col("k").isNotNull())
    .withColumn("created_at_hour_avg_1h_minus_created_at_hour_max_30m", (F.col("created_at_hour_avg_1h") - F.col("created_at_hour_max_30m")))
)

filtered = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "k",
        "created_at_hour_sum_15m",
        "created_at_hour_avg_1h",
        "created_at_hour_max_30m",
        F.round("created_at_hour_max_30m", 0).alias("created_at_hour_max_30m_rounded"),
        F.round("created_at_hour_avg_1h", 1).alias("created_at_hour_avg_1h_rounded"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled"),
    )
)

scored_carts = (
    filtered
    .select(
        "created_at_hour_avg_1h",
        "created_at_hour_max_30m",
        "created_at_hour_max_30m_rounded",
        "created_at_hour_avg_1h_rounded",
        F.sum("created_at_hour_sum_15m").over(w4).alias("k_sum_created_at_hour_sum_15m"),
        F.rank().over(w5).alias("created_at_hour_avg_1h_rank"),
        F.cume_dist().over(w6).alias("created_at_hour_max_30m_rounded_cume_dist"),
    )
)

filtered_carts = (
    scored_carts
    .filter(F.col("created_at_hour_avg_1h_rounded") <= 54)
    .select(
        "created_at_hour_avg_1h",
        "created_at_hour_max_30m_rounded",
        "created_at_hour_avg_1h_rounded",
        "k_sum_created_at_hour_sum_15m",
        "created_at_hour_avg_1h_rank",
        "created_at_hour_max_30m_rounded_cume_dist",
        (F.col("created_at_hour_avg_1h_rank") - F.col("created_at_hour_max_30m_rounded")).alias("created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded"),
    )
)

result = (
    filtered_carts
    .select(
        "*",
        F.dense_rank().over(w7).alias("created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded_dense_rank"),
        F.percent_rank().over(w8).alias("created_at_hour_avg_1h_rank_percent_rank"),
        F.percent_rank().over(w9).alias("created_at_hour_avg_1h_rank_minus_created_at_hour_max_30m_rounded_percent_rank"),
    )
)
