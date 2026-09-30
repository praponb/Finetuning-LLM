from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.month(F.col("accounts.opened_date")).isin([3, 6, 10]))
    .groupBy(F.col("customers.segment"))
    .agg(
        F.round(F.stddev(F.col("transactions.amount")), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("transactions.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .filter(F.col("debit_count") > 76)
    .orderBy("segment")
)
