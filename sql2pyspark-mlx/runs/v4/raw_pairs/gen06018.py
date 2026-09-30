from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("accounts.balance").between(34000, 67000))
    .groupBy(
        F.when(F.col("accounts.status").isin(["OPEN", "CLOSED"]), "group_a").otherwise("other").alias("status_group"),
        F.col("accounts.status"),
    )
    .agg(
        F.sum(F.when(F.col("transactions.txn_type") == "DEBIT", F.col("transactions.amount")).otherwise(0)).alias("debit_amount"),
    )
    .orderBy("debit_amount")
)
