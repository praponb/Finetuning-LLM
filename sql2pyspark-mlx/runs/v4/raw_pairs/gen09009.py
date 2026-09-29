from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isin([4, 7, 22, 36]) | (F.col("balance") >= 40000))
)

accounts_stats = (
    base
    .groupBy(
        F.when(F.col("balance") >= 71000, "high").when(F.col("balance") >= 34000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
)

result = (
    accounts_stats
    .select("balance_bucket", "account_type", "total_balance", "frozen_count")
)
