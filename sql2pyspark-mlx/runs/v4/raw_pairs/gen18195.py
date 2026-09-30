from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 40) & (F.col("shipping_fee").isNotNull() | (F.col("amount") <= 730)))
    .select(
        "channel",
        "store_id",
        "order_date",
        F.round("discount", 2).alias("discount_rounded"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
    .orderBy(F.col("amount_per_discount").desc())
)
