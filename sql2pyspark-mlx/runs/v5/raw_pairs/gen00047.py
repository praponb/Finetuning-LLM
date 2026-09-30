from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("store_id").isin([10, 25, 27, 30]))

orders_stats = (
    valid_orders
    .groupBy(
        F.when(F.col("discount") >= 0.33, "high").when(F.col("discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        F.max("order_date").alias("latest_order_date"),
    )
)

result = orders_stats
