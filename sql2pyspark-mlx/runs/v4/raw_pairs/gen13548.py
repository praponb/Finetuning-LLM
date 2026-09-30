from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

result = (
    flattened
    .select("id", "created_at", "coupon_codes", "k", "v")
    .orderBy("k")
)
