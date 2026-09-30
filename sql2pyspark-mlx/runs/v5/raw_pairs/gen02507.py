from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") >= "2024-06-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("attributes").alias("num_attributes"),
    )
)
