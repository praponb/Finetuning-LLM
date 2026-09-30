from pyspark.sql import functions as F

base = df_accounts.filter(F.col("balance") > 44000)

accounts_stats = (
    base
    .groupBy("status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    accounts_stats
    .filter(F.col("total_balance") >= 472000)
    .select("status", "max_balance", "total_balance")
    .orderBy("status")
)
