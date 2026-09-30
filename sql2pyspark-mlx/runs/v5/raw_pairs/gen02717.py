from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.36) | ~F.col("status").isin(["RETURNED", "SHIPPED"]))
    .select(
        "status",
        "customer_id",
        (F.col("discount") + F.col("amount")).alias("discount_plus_amount"),
        F.substring("channel", 1, 3).alias("channel_prefix"),
    )
)
