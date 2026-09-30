from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.21)
    .select(
        "id",
        "order_date",
        "customer_id",
        "store_id",
        F.lower("channel").alias("channel_lower"),
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
    )
)
