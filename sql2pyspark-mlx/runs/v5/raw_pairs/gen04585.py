from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS"]))
    .groupBy("status", "account_type")
    .agg(F.count("balance").alias("balance_count"))
)
