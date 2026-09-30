from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.completed_count") >= 60)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("t.latest_order_date"),
        F.col("t.completed_count"),
    )
)
