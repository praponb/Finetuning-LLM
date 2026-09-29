from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & (F.col("balance") > 41000) & (F.col("balance") >= 22000))
    .groupBy("account_type")
    .agg(F.count("balance").alias("balance_count"))
    .orderBy(F.col("balance_count").desc(), "account_type")
    .limit(3)
)
