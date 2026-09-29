from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & (F.col("account_type") == "LOAN"))
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 13000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
)
