from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.is_active"))
    .groupBy(F.col("customers.is_active"), F.col("customers.country"))
    .agg(F.max(F.col("orders.order_date")).alias("last_order_date"))
    .orderBy("country")
)
