from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .select("id", "created_at", "coupon_codes", "product_id")
    .orderBy(F.col("created_at").asc_nulls_first())
)
