from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.orderBy(F.col("created_at_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.orderBy("days_since_created_at")
w4 = Window.orderBy(F.col("adjusted_days_since_created_at").desc_nulls_last())
w5 = Window.orderBy(F.col("days_since_created_at_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("days_since_created_at_cume_dist_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("created_at_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.orderBy(F.col("days_since_created_at_cume_dist_dense_rank").desc_nulls_last())
w9 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w10 = Window.orderBy(F.col("days_since_created_at_cume_dist").desc_nulls_last())

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("k") == "utm_source")
    .select(
        "created_at",
        "value",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.date_format("created_at", "MMM yyyy").alias("created_at_label"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled"),
    )
)

valid = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & (F.col("days_since_created_at") > 25))
    .select(
        "created_at",
        "days_since_created_at",
        "created_at_label",
        "k_filled",
        F.round((F.col("days_since_created_at") * 1.15), 2).alias("adjusted_days_since_created_at"),
        F.date_trunc("month", "created_at").alias("created_at_period"),
    )
)

scored = (
    valid
    .withColumn("adjusted_days_since_created_at_max_15m", F.max("adjusted_days_since_created_at").over(w))
    .withColumn("days_since_created_at_max_90d", F.max("days_since_created_at").over(w2))
    .withColumn("days_since_created_at_cume_dist", F.cume_dist().over(w3))
)

scored_carts = (
    scored
    .withColumn("adjusted_days_since_created_at_percent_rank", F.percent_rank().over(w4))
    .withColumn("days_since_created_at_cume_dist_dense_rank", F.dense_rank().over(w5))
)

scored_carts_2 = (
    scored_carts
    .filter(F.col("created_at_period") >= "2025-01-06")
    .select(
        "created_at",
        "days_since_created_at",
        "created_at_label",
        "days_since_created_at_max_90d",
        "days_since_created_at_cume_dist",
        "adjusted_days_since_created_at_percent_rank",
        "days_since_created_at_cume_dist_dense_rank",
        F.sum("days_since_created_at_max_90d").over(w2).alias("days_since_created_at_max_90d_sum_90d"),
        F.cume_dist().over(w6).alias("days_since_created_at_cume_dist_dense_rank_cume_dist"),
        F.max("days_since_created_at").over(w7).alias("days_since_created_at_max_7d"),
    )
)

metrics = (
    scored_carts_2
    .select(
        "*",
        F.max("days_since_created_at_cume_dist").over(w).alias("days_since_created_at_cume_dist_max_15m"),
        F.max("adjusted_days_since_created_at_percent_rank").over(w).alias("adjusted_days_since_created_at_percent_rank_max_15m"),
        F.percent_rank().over(w8).alias("days_since_created_at_cume_dist_dense_rank_percent_rank"),
        F.avg("days_since_created_at_max_90d").over(w9).alias("days_since_created_at_max_90d_avg_2h"),
        F.percent_rank().over(w10).alias("days_since_created_at_cume_dist_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "created_at",
        "days_since_created_at",
        "created_at_label",
        "days_since_created_at_max_90d",
        "days_since_created_at_cume_dist",
        "adjusted_days_since_created_at_percent_rank",
        "days_since_created_at_cume_dist_dense_rank",
        "days_since_created_at_max_90d_sum_90d",
        "days_since_created_at_cume_dist_dense_rank_cume_dist",
    )
    .orderBy(F.col("created_at").desc_nulls_last())
)
