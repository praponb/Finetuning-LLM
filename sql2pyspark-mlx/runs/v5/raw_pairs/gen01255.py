from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.amount") > 770)
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(F.count(F.col("orders.id")).alias("order_count"))
)
