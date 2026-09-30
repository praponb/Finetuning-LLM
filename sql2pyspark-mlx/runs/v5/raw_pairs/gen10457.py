from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "amount",
        "status",
        F.least("amount", "shipping_fee").alias("least_amount_shipping_fee"),
    )
)
