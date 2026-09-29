from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        F.countDistinct("balance").alias("unique_customers"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.country"),
        F.col("cs.min_balance"),
        F.col("cs.unique_customers"),
    )
)
