from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = flattened.select("id", "coupon_codes", "attr_key", "attr_value")
