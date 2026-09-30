from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.country") == "TH")
    .groupBy(
        F.when(F.col("a.balance") >= 63000, "high").when(F.col("a.balance") >= 18000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("t.txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)
