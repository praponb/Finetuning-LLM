from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("transactions.amount") > 3600)
    .select(
        F.col("transactions.txn_time"),
        F.col("accounts.balance"),
        F.col("accounts.status"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("status").desc())
)
