from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(
        F.col("a.status"),
        F.when(F.col("a.balance") >= 61000, "high").when(F.col("a.balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count(F.when(F.col("t.merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
    )
)
