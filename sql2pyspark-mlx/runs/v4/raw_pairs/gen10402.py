from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.size("coupon_codes") >= 1)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("attributes").getItem("device").alias("device"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)
