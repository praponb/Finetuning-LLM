from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.when(F.col("balance") >= 71000, "high").when(F.col("balance") >= 21000, "mid").otherwise("low").alias("balance_bucket"))
    .pivot("account_type", ["SAVINGS", "CREDIT"])
    .agg(F.sum("balance"))
)
