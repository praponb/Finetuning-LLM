from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

result = lines.select("id", "coupon_codes", "product_id", "qty")
