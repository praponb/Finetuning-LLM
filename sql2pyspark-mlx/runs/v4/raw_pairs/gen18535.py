from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc())
w_running = Window.partitionBy("promo", "tag").orderBy(F.col("price").asc_nulls_first())
w3 = Window.partitionBy("tag", "promo").orderBy(F.col("price").desc_nulls_last())
w4 = Window.partitionBy("promo")
w_recent = Window.partitionBy("tag")
w_part = Window.orderBy(F.col("price").desc())

flattened = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

flattened_carts = (
    flattened
    .select("product_id", "price", "promo", F.explode("coupon_codes").alias("tag"))
)

metrics = (
    flattened_carts
    .select(
        "*",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.rank().over(w_running).alias("price_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.round((F.col("price_rank") / F.sum("price_rank").over(w4)), 4).alias("price_rank_share"),
    )
)

cleaned = (
    enriched
    .filter(F.col("promo").isNotNull() & F.col("price_rank_share").isNotNull())
    .withColumn("price_rank_share_plus_product_id_cume_dist", (F.col("price_rank_share") + F.col("product_id_cume_dist")))
)

base = (
    cleaned
    .filter(F.col("promo").isNotNull() & F.col("price_rank").isNotNull())
    .select(
        "product_id",
        "price",
        "promo",
        "tag",
        "price_percent_rank",
        "price_rank_share",
        "price_rank_share_plus_product_id_cume_dist",
        F.round((F.col("product_id") * 100), 2).alias("adjusted_product_id"),
        F.ceil("price_rank").alias("price_rank_whole"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_clean"),
    )
)

scored = (
    base
    .withColumn("tag_avg_price_rank_share", F.avg("price_rank_share").over(w_recent))
    .withColumn("tag_min_price", F.min("price").over(w_recent))
    .withColumn("price_cume_dist", F.cume_dist().over(w_part))
    .withColumn("tag_count_price_percent_rank", F.count("price_percent_rank").over(w_recent))
)

result = (
    scored
    .groupBy(
        F.when(F.col("price_percent_rank") >= 0.74, "LARGE").otherwise("SMALL").alias("price_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("price_percent_rank", 0.5).alias("price_percent_rank_p50"),
        F.countDistinct("promo").alias("distinct_promos"),
    )
)
