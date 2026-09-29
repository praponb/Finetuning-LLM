from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "inner")
    .select(F.col("c.age"), F.col("obc.completed_count"), F.col("obc.mobile_count"))
)
