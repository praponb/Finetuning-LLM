from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)
