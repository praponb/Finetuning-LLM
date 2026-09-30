from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

scored = (
    exploded
    .filter(F.col("product_id").between(7, 9))
    .withColumn("promo_avg_product_id", F.avg("product_id").over(w))
    .withColumn("price_dense_rank", F.dense_rank().over(w2))
    .withColumn("promo_count_product_id", F.count("product_id").over(w))
)

scored_carts = (
    scored
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "item_no",
        "product_id",
        "price",
        "promo_avg_product_id",
        "promo_count_product_id",
        F.count("product_id").over(w3).alias("product_id_count_15m"),
        F.min("item_no").over(w).alias("promo_min_item_no"),
    )
)

cleaned = (
    scored_carts
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.when(F.col("promo_min_item_no") >= 3, "High").when(F.col("promo_min_item_no") >= 1, "Medium").otherwise("Low").alias("promo_min_item_no_level"),
        F.round("item_no", 1).alias("item_no_rounded"),
    )
)

kept_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.round(F.avg("promo_min_item_no"), 2).alias("avg_promo_min_item_no"),
        F.percentile_approx("item_no_rounded", 0.9).alias("item_no_rounded_p90"),
        F.min("created_at").alias("first_created_at"),
        F.percentile_approx("promo_count_product_id", 0.9).alias("promo_count_product_id_p90"),
    )
)

result = (
    grouped
    .select(
        "activity_day",
        "avg_promo_min_item_no",
        "item_no_rounded_p90",
        "promo_count_product_id_p90",
    )
    .orderBy("item_no_rounded_p90")
)
