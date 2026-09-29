from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.avg("discount").alias("avg_discount"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.avg_discount"))
)
