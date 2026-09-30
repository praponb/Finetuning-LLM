from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select("id", "coupon_codes", "pos", F.col("item.product_id"), F.col("item.promo"))
)

result = (
    lines
    .select("id", "pos", "product_id", "promo")
    .orderBy(F.col("product_id").asc_nulls_first(), "id")
)
