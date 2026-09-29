from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.customer_id").isin([3, 15, 16, 39]))
    .groupBy(
        F.when(F.col("orders.amount") >= 1450, "high").when(F.col("orders.amount") >= 380, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.col("shipments.cost")).alias("cost_count"),
        F.round(F.stddev(F.col("shipments.cost")), 2).alias("stddev_cost"),
    )
    .orderBy("amount_bucket")
)
