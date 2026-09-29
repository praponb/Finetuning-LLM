from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2023-04-01") & F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
        F.max("balance").alias("max_balance"),
    )
)
