from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(~F.col("t.txn_type").isin(["DEBIT", "FEE"]) & F.col("a.customer_id").isNull())
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
    )
    .orderBy("txn_type", F.col("id").desc())
)
