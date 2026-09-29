from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("s.shipped_date")).isin([3, 8, 12]))
    .groupBy(F.col("c.country"))
    .agg(
        F.avg(F.col("s.cost")).alias("avg_cost"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("cost_range").desc())
)
