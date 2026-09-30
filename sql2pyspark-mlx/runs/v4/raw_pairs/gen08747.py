from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") >= 297000)
    .orderBy("status")
)
