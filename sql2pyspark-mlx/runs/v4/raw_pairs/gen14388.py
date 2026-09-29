from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 54)
    .groupBy(F.col("c.city"))
    .agg(F.min(F.col("o.order_date")).alias("first_order_date"))
)
