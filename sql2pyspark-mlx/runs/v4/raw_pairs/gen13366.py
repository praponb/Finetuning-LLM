from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .select("id", "created_at", "coupon_codes", "price")
    .orderBy(F.col("price").desc())
)
