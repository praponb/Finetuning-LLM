from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 35000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") < 58000)
)
