from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") <= 22) & (F.col("channel") == "web")) | (F.col("shipping_fee") > 37))
    .select(
        "id",
        "channel",
        "amount",
        "store_id",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
    )
    .orderBy(F.col("amount").desc(), F.col("id").desc())
)
