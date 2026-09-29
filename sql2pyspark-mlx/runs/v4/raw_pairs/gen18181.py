from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("oi.unit_price").between(170, 330))
    .groupBy(
        F.when(F.col("o.amount") >= 1360, "high").when(F.col("o.amount") >= 520, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("amount_bucket", F.col("avg_quantity").desc())
)
