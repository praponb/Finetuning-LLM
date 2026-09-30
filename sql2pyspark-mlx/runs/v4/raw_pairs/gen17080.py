from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.merchant") == "Amazon")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", F.col("t.amount")).otherwise(0)).alias("debit_amount"),
    )
    .orderBy("account_type", F.col("debit_amount").desc())
)
