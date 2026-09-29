from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & (F.col("balance") >= 79000))
)

status_summary = (
    recent_accounts
    .groupBy("status")
    .agg(F.avg("balance").alias("avg_balance"))
)

result = status_summary.orderBy("status", F.col("avg_balance").desc())
