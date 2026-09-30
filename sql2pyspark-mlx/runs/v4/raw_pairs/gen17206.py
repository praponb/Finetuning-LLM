from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
)
