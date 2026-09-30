from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = lines.select("id", "coupon_codes", "qty", "price", "promo")
