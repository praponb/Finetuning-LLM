from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]) & (F.col("balance") <= 66000))
    .groupBy("account_type", "customer_id")
    .agg(F.count("balance").alias("balance_count"))
)
