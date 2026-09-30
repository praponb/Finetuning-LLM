from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 21)
    .select(
        "discount",
        "channel",
        "status",
        F.substring("channel", 1, 4).alias("channel_prefix"),
    )
    .orderBy(F.col("discount").desc())
)
