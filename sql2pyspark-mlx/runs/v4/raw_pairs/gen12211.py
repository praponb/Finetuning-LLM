from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.avg("balance").alias("avg_balance"))
    .filter(F.col("avg_balance") >= 28000)
    .orderBy("avg_balance", F.col("account_type").desc())
    .limit(5)
)
