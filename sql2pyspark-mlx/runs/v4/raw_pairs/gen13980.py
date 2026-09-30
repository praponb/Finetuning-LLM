from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "coupon_codes", "key", "attr_value")
    .orderBy(F.col("key").desc(), "id")
)
