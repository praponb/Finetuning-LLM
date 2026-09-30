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
        "coupon_codes",
        "attr_key",
        F.when(F.col("attr_key").rlike("^(weight_kg|device)$"), "PRIORITY").when(F.col("attr_key").rlike("^(utm_source|gift)$"), "EXTENDED").otherwise("GROUP_B").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key").desc(), F.col("attr_key_zone").desc())
)
