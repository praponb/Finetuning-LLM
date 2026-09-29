from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.customer_id").isNotNull())
    .groupBy(F.col("a.account_type"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.count(F.when(F.col("t.merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .filter(F.col("pct_credit") >= 79)
)
