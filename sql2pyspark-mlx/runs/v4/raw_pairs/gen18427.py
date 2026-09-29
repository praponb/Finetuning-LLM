from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.when(F.col("orders.discount") >= 0.39, "high").when(F.col("orders.discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.round(F.avg(F.col("shipments.cost")), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") > 77)
)
