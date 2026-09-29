from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "customer_id", "created_at", "coupon_codes", "attr_key", "attr_value")
)
