from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .groupBy("customer_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "left")
    .filter(F.col("obc.amount_count") > 8)
    .select(F.col("c.credit_limit"), F.col("c.country"), F.col("obc.amount_count"))
    .orderBy("country", "credit_limit")
)
