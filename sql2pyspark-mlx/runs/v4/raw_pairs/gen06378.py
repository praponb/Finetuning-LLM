from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 42000)
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .filter(F.col("balance_count") < 6)
)
