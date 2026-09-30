from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("amount") >= 1400) & F.col("store_id").isin([20, 32]))
    .select(
        "id",
        "channel",
        "shipping_fee",
        "amount",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy("channel", "amount")
)
