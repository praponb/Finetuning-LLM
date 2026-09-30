from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .groupBy("customer_id")
    .agg(F.countDistinct("amount").alias("unique_statuss"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_statuss") > 8)
    .select(F.col("c.email"), F.col("c.credit_limit"), F.col("cs.unique_statuss"))
)
