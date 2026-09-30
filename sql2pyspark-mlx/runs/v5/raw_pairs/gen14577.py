from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("status").isin(["COMPLETED", "SHIPPED", "PENDING"]) & F.col("discount").between(0.11, 0.15) & F.col("status").isin(["COMPLETED", "PENDING", "CANCELLED"])))
    .select(
        "customer_id",
        "amount",
        "channel",
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
    )
)
