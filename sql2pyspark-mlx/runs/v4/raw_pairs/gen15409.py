from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(weight_kg|device)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc(), "attr_key_zone")
)
