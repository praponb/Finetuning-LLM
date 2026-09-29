from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .select(
        "id",
        "channel",
        "store_id",
        (F.col("discount") / F.nullif("shipping_fee", F.lit(0))).alias("discount_per_shipping_fee"),
    )
)
