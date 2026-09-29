from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("customer_id")
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .filter(F.col("abc.total_balance") >= 235000)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.name"),
        F.col("abc.total_balance"),
    )
    .orderBy("total_balance")
)
