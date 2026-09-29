from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
    )
    .orderBy("pct_starbucks", "account_type")
    .limit(3)
)
