from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        "v",
        F.when(F.col("key").rlike("^(gift|weight_kg)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("key").desc_nulls_last(), F.col("id").desc())
)
