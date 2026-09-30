from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 8, 28, 37]))
    .groupBy("account_type")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") < 207000)
    .select("account_type", "avg_balance", "total_balance")
)
