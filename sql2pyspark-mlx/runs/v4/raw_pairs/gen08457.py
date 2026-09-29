from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.status").isin(["CANCELLED", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg((F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"))
)
