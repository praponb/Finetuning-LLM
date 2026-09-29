from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("tag")

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = exploded.select("qty", "price", F.explode("coupon_codes").alias("tag"))

enriched = (
    flattened
    .select(
        "*",
        F.rank().over(w).alias("qty_rank"),
        F.avg("price").over(w2).alias("tag_avg_price"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)

grouped = (
    enriched
    .groupBy("tag")
    .agg(
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_rows"),
    )
)

result = (
    grouped
    .select("tag", "spring10_rows")
    .orderBy(F.col("tag").desc_nulls_first())
)
