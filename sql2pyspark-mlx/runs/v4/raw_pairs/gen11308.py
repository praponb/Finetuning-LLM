from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") | (F.col("customer_id") == 23))
    .groupBy(
        F.when(F.col("balance") >= 59000, "high").when(F.col("balance") >= 24000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
    .orderBy("loan_balance", "balance_bucket")
    .limit(1)
)
