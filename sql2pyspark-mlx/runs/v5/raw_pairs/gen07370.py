from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .filter(~F.col("t.txn_type").isin(["FEE", "DEBIT", "REFUND"]))
    .groupBy(F.col("a.account_type"), F.col("a.status"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.sum(F.when(F.col("t.merchant") == "Amazon", F.col("t.amount")).otherwise(0)).alias("amazon_amount"),
    )
)
