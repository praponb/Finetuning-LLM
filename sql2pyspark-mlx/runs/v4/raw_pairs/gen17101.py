from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

flattened = df_carts.select("id", "coupon_codes", F.inline("items"))

result = (
    flattened
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
        F.avg("product_id").over(w).alias("promo_avg_product_id"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share_2"),
    )
)
