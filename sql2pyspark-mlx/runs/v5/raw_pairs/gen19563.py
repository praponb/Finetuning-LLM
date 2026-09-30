from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 14, 17, 37]))
    .select(
        "store_id",
        "status",
        F.least("shipping_fee", "amount").alias("least_shipping_fee_amount"),
        F.substring("channel", 1, 3).alias("channel_prefix"),
    )
)
