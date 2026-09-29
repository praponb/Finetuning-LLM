from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("qty").desc_nulls_first())
w3 = Window.partitionBy("customer_id", "promo")
w_hist = Window.partitionBy("promo")

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.rank().over(w2).alias("qty_rank"),
        F.max("qty").over(w3).alias("customer_id_max_qty"),
        F.min("qty").over(w_hist).alias("promo_min_qty"),
    )
)
