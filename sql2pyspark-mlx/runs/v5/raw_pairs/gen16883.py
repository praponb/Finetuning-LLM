from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy("price")
w_part = Window.partitionBy("tag").orderBy(F.col("price").desc_nulls_last())
w_running = Window.partitionBy("tag")
w4 = Window.orderBy(F.col("price").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines = flattened.select("id", "price", F.explode("coupon_codes").alias("tag"))

enriched = lines.withColumn("price_cume_dist", F.cume_dist().over(w))

enriched_carts = (
    enriched
    .filter(F.col("price_cume_dist").between(0.2, 0.39))
    .select(
        "*",
        F.percent_rank().over(w_part).alias("price_percent_rank"),
        F.count("price_cume_dist").over(w_running).alias("tag_count_price_cume_dist"),
        F.rank().over(w4).alias("price_rank"),
        F.sum("price").over(w_running).alias("tag_sum_price"),
        F.round((F.col("price") / F.sum("price").over(w_running)), 4).alias("price_share"),
    )
)

result = (
    enriched_carts
    .select(
        "id",
        "price",
        "tag",
        "price_cume_dist",
        "tag_count_price_cume_dist",
        "price_share",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
