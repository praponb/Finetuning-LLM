from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("element").orderBy("price")
w3 = Window.orderBy(F.col("price_cume_dist").desc_nulls_last())
w4 = Window.orderBy("price_cume_dist")

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded_carts = exploded.select("id", "price", "promo", F.explode("coupon_codes").alias("element"))

enriched = (
    exploded_carts
    .select(
        "price",
        "promo",
        F.min("price").over(w).alias("promo_min_price"),
        F.cume_dist().over(w2).alias("price_cume_dist"),
    )
)

scored = (
    enriched
    .select(
        "price",
        "price_cume_dist",
        F.cume_dist().over(w3).alias("price_cume_dist_cume_dist"),
        F.cume_dist().over(w4).alias("price_cume_dist_cume_dist_2"),
    )
)

daily = (
    scored
    .withColumn("price_cume_dist_cume_dist_2_tier", F.when(F.col("price_cume_dist_cume_dist_2") >= 0.81, "HIGH").otherwise("LOW"))
    .withColumn("price_cume_dist_cume_dist_tier", F.when(F.col("price_cume_dist_cume_dist") >= 0.61, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["price_cume_dist_cume_dist_2_tier", "price_cume_dist_cume_dist_tier"], ["price_cume_dist_cume_dist_2_tier"], []],
        "price_cume_dist_cume_dist_2_tier",
        "price_cume_dist_cume_dist_tier",
    )
    .agg(
        F.round(F.avg("price_cume_dist_cume_dist_2"), 2).alias("avg_price_cume_dist_cume_dist_2"),
        F.count("*").alias("event_count"),
        F.percentile_approx("price_cume_dist", 0.5).alias("price_cume_dist_p50"),
        F.grouping("price_cume_dist_cume_dist_2_tier").alias("grouping_level"),
    )
)

result = (
    daily
    .select(
        "price_cume_dist_cume_dist_2_tier",
        "price_cume_dist_cume_dist_tier",
        "avg_price_cume_dist_cume_dist_2",
        "event_count",
        "price_cume_dist_p50",
        "grouping_level",
    )
)
