from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    flattened
    .select("customer_id", "created_at", "code")
    .orderBy(F.col("created_at").desc_nulls_first())
)
