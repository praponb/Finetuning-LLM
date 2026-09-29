from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17700, "high").when(F.col("c.credit_limit") >= 5400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
)
