from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(F.col("a.account_type"), F.col("a.customer_id"), F.col("at.debit_count"))
    .orderBy(F.col("debit_count").desc(), F.col("customer_id").desc())
)
