from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.month("opened_date").isin([3, 5, 9]))
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("abc.avg_balance"),
        F.col("abc.savings_count"),
    )
    .orderBy("avg_balance")
)
