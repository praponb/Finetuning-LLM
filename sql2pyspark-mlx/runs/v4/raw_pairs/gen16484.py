from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)
