from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "right")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("transactions.merchant"),
        F.col("accounts.status"),
        F.col("accounts.balance"),
        F.col("customers.country"),
    )
    .orderBy(F.col("status").desc())
)
