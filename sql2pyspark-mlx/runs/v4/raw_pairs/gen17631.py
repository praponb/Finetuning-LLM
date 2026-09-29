from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo").orderBy(F.col("qty").asc_nulls_first())
w2 = Window.orderBy(F.col("qty").desc_nulls_first())
w_ordered = Window.orderBy("product_id_whole")
w_hist = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("price_count_30m").desc())
w7 = Window.partitionBy("promo", "customer_id")
w8 = Window.orderBy(F.col("product_id_whole").asc_nulls_last())

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

valid = (
    exploded
    .filter(F.col("created_at") >= F.lit("2025-03-02 00:00:00").cast("timestamp"))
    .withColumn("product_id_whole", F.ceil("product_id"))
)

metrics = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.rank().over(w2).alias("qty_rank"),
        F.cume_dist().over(w_ordered).alias("product_id_whole_cume_dist"),
        F.avg("product_id").over(w_hist).alias("product_id_avg_30m"),
        F.count("price").over(w5).alias("price_count_30m"),
    )
)

cleaned = (
    metrics
    .filter(F.col("promo") == "SPRING10")
    .select(
        "*",
        (F.col("price") * F.col("product_id_whole")).alias("price_times_product_id_whole"),
        F.array_join("coupon_codes", "|").alias("coupon_codes_text"),
    )
)

enriched = (
    cleaned
    .withColumn("price_count_30m_cume_dist", F.cume_dist().over(w6))
    .withColumn("promo_sum_price_count_30m", F.sum("price_count_30m").over(w7))
    .withColumn("product_id_whole_dense_rank", F.dense_rank().over(w8))
)

summary = (
    enriched
    .groupBy(F.when(F.col("price") >= 229, "HIGH").otherwise("LOW").alias("price_tier"))
    .agg(
        F.round(F.sum("product_id_whole_dense_rank") / F.nullif(F.count(F.when(F.col("promo") == "VIP", 1)), F.lit(0)), 2).alias("product_id_whole_dense_rank_per_vip"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.percentile_approx("product_id_whole_dense_rank", [0.25, 0.5, 0.75]).alias("product_id_whole_dense_rank_quartiles"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.min("created_at").alias("first_created_at"),
        F.size(F.collect_set("promo")).alias("num_promos"),
    )
)

result = (
    summary
    .select(
        "price_tier",
        "product_id_whole_dense_rank_per_vip",
        "price_p50",
        "product_id_whole_dense_rank_quartiles",
        "qty_quartiles",
        "first_created_at",
        "num_promos",
    )
    .orderBy(F.col("num_promos").desc())
)
