from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "customer_id",
        "k",
        F.when(F.col("k").rlike("^(gift|utm_source)$"), "EXTENDED").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("id").desc())
)
