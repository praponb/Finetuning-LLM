from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 830) & F.col("customer_id").isNotNull() & (F.col("status") == "RETURNED"))
    .select(
        "id",
        "channel",
        "order_date",
        "amount",
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.least("amount", "shipping_fee").alias("least_amount_shipping_fee"),
    )
)
