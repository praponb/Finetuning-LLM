from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "discount",
        "channel",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        (F.col("shipping_fee") / F.nullif("discount", F.lit(0))).alias("shipping_fee_per_discount"),
    )
)
