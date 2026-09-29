from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("customer_id").isin([1, 11, 16]))
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.country"), F.col("t.total_count"))
    .orderBy("total_count")
)
