from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.city"))
    .agg(
        F.sum(F.when(F.col("transactions.merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)
