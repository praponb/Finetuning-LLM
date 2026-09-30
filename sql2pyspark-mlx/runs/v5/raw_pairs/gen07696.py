from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .groupBy("customer_id")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.avg_balance") > 57)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("cs.avg_balance"),
    )
    .orderBy("id")
)
