from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)
