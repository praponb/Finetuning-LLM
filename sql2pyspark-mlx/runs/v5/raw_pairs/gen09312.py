from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .filter(F.col("account_totals.debit_count") > 51)
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.customer_id"),
        F.col("account_totals.debit_count"),
        F.col("account_totals.last_txn_time"),
    )
    .orderBy(F.col("customer_id").desc())
)
