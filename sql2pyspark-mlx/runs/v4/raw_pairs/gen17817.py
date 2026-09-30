from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.discount_count") > 10)
    .select(F.col("c.is_active"), F.col("t.discount_count"))
)
