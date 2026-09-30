from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(23, 24) & (F.col("order_date") > "2024-04-01"))
    .groupBy("channel", F.month("order_date").alias("order_month"))
    .agg(
        F.count("discount").alias("discount_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sum("amount").alias("total_amount"),
    )
)
