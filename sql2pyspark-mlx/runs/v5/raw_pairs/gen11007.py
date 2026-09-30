from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attr_key",
        "attr_value",
        F.when(F.col("attr_key").rlike("^(weight_kg|device)$"), "GROUP_A").when(F.col("attr_key").rlike("^(utm_source|gift)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_zone").desc_nulls_first())
)
