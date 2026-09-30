from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_first())
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("price").desc_nulls_first())

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    exploded
    .select(
        "qty",
        "price",
        "promo",
        F.cume_dist().over(w).alias("qty_cume_dist"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.max("price").over(w2).alias("promo_max_price"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
    )
)
