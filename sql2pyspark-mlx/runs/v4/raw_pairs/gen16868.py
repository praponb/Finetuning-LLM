from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .groupBy("customer_id")
    .agg(F.avg("discount").alias("avg_discount"))
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.country"), F.col("t.avg_discount"))
    .orderBy(F.col("avg_discount").desc(), F.col("id").desc())
)
