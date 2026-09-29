from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)
