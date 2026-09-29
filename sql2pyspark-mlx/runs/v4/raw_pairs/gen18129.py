from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("customer_id").isin([9, 13, 21, 24]) & ((F.col("discount") <= 0.15) | F.col("amount").between(410, 1500)))
)

orders_stats = (
    valid_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.avg("discount").alias("avg_discount"))
)

result = (
    orders_stats
    .filter(F.col("avg_discount") >= 0.1)
    .select("order_year", "avg_discount")
)
