from pyspark.sql import functions as F

base = df_orders.filter(F.col("shipping_fee").between(18, 38))

orders_stats = (
    base
    .groupBy("status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = orders_stats
