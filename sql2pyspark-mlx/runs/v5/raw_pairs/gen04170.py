from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(31, 37) & (F.col("discount") >= 0.13))
    .select(
        "amount",
        "channel",
        "order_date",
        F.greatest("amount", "shipping_fee").alias("greatest_amount_shipping_fee"),
    )
)
