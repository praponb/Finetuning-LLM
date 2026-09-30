from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 79000, "high").when(F.col("balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
    .filter(F.col("pct_savings") < 46)
    .orderBy(F.col("pct_savings").desc(), "balance_bucket")
)
