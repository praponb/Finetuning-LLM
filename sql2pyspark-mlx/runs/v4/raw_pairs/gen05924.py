from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w_recent = Window.orderBy(F.col("qty").desc_nulls_first())
w3 = Window.partitionBy("promo").orderBy(F.col("price_share").desc_nulls_first())
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_hist = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())
w6 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy("promo").orderBy(F.col("qty_percent_rank").desc())
w8 = Window.partitionBy("promo").orderBy(F.col("price_share_dense_rank").desc_nulls_first())
w9 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

enriched = (
    flattened
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
        F.max("qty").over(w).alias("promo_max_qty"),
        F.sum("price").over(w).alias("promo_sum_price"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("qty_percent_rank"),
        F.dense_rank().over(w3).alias("price_share_dense_rank"),
        F.count("price").over(w4).alias("price_count_30m"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([5, 21, 29]))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_carts = (
    reconciled
    .withColumn("qty_rank", F.rank().over(w_hist))
    .withColumn("promo_max_promo_max_qty", F.max("promo_max_qty").over(w))
    .withColumn("price_sum_15m", F.sum("price").over(w6))
)

valid = (
    metrics_carts
    .filter((F.col("created_at") >= F.lit("2025-03-02 09:00:00").cast("timestamp")) & F.col("price_count_30m").isNotNull())
    .select(
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        "price_share",
        "price_share_2",
        "promo_max_qty",
        "qty_percent_rank",
        "price_share_dense_rank",
        "price_count_30m",
        "promo_max_promo_max_qty",
        "price_sum_15m",
        F.round((F.col("qty") / F.col("product_id")), 2).alias("qty_per_product_id"),
    )
)

enriched_carts = (
    valid
    .select(
        "*",
        F.cume_dist().over(w7).alias("qty_percent_rank_cume_dist"),
        F.dense_rank().over(w8).alias("price_share_dense_rank_dense_rank"),
        F.count("price_sum_15m").over(w9).alias("price_sum_15m_count_30m"),
    )
)

result = (
    enriched_carts
    .select(
        "promo",
        F.col("price_share_2").alias("PRICE_SHARE_2_VAL"),
        "qty_percent_rank",
    )
    .unpivot(
        ["promo"],
        ["PRICE_SHARE_2_VAL", "qty_percent_rank"],
        "kpi",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
