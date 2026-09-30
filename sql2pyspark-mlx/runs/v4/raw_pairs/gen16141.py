from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
    .filter(F.col("cost_count") >= 3)
    .orderBy(F.col("cost_count").desc())
)
