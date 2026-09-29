from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .select(
        "created_at",
        "price",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.count("price").over(w).alias("promo_count_price"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
    )
)
