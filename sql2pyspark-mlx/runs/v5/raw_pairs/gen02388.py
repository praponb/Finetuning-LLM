from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.total_amount") > 9200)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("cs.total_amount"),
        F.col("cs.avg_amount"),
    )
    .orderBy("avg_amount")
)
