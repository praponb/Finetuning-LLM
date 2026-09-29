from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "amount",
        "store_id",
        "channel",
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
)
