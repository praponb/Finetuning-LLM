from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 49000)
    .agg(F.sum("balance").alias("total_balance"))
)
