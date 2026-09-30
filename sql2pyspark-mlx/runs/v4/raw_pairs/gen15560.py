from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "coupon_codes",
        "key",
        "v",
        F.when(F.col("key").rlike("^(gift|utm_source)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("key_zone"),
    )
)
