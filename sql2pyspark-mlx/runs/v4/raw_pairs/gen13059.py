from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy("qty")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_first())
w6 = Window.partitionBy("promo").orderBy(F.col("qty_rank_count_2h").desc())
w7 = Window.partitionBy("promo").orderBy(F.col("promo_max_qty").desc_nulls_first())
w_running = Window.orderBy(F.col("qty_rank_count_2h_percent_rank").desc_nulls_first())
w9 = Window.partitionBy("promo").orderBy(F.col("qty_rank_count_2h_percent_rank").desc_nulls_first())
w10 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

metrics = (
    lines
    .withColumn("promo_max_qty", F.max("qty").over(w))
    .withColumn("qty_rank", F.rank().over(w2))
)

valid = (
    metrics
    .filter((F.col("created_at") > F.lit("2025-03-02 14:00:00").cast("timestamp")) & F.col("promo").isNotNull() & F.hour("created_at").between(7, 12))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        "promo_max_qty",
        "qty_rank",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.count("qty_rank").over(w3).alias("qty_rank_count_2h"),
        F.sum("product_id").over(w).alias("promo_sum_product_id"),
        F.count("product_id").over(w4).alias("product_id_count_30m"),
        F.percent_rank().over(w5).alias("product_id_percent_rank"),
    )
)

with_windows_carts = (
    with_windows
    .select(
        "*",
        F.avg("promo_max_qty").over(w).alias("promo_avg_promo_max_qty"),
        F.percent_rank().over(w6).alias("qty_rank_count_2h_percent_rank"),
    )
)

scored = (
    with_windows_carts
    .withColumn("qty_rank_count_2h_share", F.round((F.col("qty_rank_count_2h") / F.sum("qty_rank_count_2h").over(w)), 4))
    .withColumn("promo_max_qty_cume_dist", F.cume_dist().over(w7))
    .withColumn("qty_rank_count_2h_percent_rank_cume_dist", F.cume_dist().over(w_running))
    .withColumn("qty_rank_count_2h_percent_rank_rank", F.rank().over(w9))
)

scored_carts = (
    scored
    .select(
        "*",
        F.sum("product_id_count_30m").over(w10).alias("product_id_count_30m_sum_1h"),
        F.round((F.col("promo_max_qty") / F.sum("promo_max_qty").over(w)), 4).alias("promo_max_qty_share"),
    )
)

result = (
    scored_carts
    .groupBy("promo")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.round(F.sum("product_id_percent_rank") / F.nullif(F.count(F.when(F.col("promo") == "SPRING10", 1)), F.lit(0)), 2).alias("product_id_percent_rank_per_spring10"),
        F.percentile_approx("promo_avg_promo_max_qty", [0.25, 0.5, 0.75]).alias("promo_avg_promo_max_qty_quartiles"),
    )
    .filter(F.col("product_id_percent_rank_per_spring10") > 0)
    .orderBy(F.col("product_id_percent_rank_per_spring10").desc())
)
