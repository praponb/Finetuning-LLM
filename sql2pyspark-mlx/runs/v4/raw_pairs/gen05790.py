from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"), F.col("c.is_active"))
    .agg(F.min(F.col("o.order_date")).alias("min_order_date"))
    .orderBy("city")
)
