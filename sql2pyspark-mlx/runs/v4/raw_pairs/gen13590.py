from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.when(F.col("a.balance") >= 80000, "high").when(F.col("a.balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", F.col("t.amount")).otherwise(0)).alias("credit_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
    .orderBy(F.col("credit_amount").desc())
)
