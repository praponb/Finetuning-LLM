from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]) & F.col("status").isNotNull() & (F.col("opened_date") < "2025-05-27"))
    .groupBy("status", "account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") < 36000)
)
