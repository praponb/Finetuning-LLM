from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .filter(F.col("O.cnt") > 5)
    .select(F.col("c.id"), F.col("c.name"), F.col("O.cnt"))
)
