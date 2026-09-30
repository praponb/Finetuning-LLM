from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "customer_id", "coupon_codes", "key")
    .orderBy(F.col("customer_id").asc_nulls_first())
)
