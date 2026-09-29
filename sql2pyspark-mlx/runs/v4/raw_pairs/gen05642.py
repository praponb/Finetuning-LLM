from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("cs.closed_count"),
    )
)
