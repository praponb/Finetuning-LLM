from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "store_id",
        "channel",
        "amount",
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
    )
    .orderBy(F.col("amount").desc())
)
