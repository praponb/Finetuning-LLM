from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
    )
)
