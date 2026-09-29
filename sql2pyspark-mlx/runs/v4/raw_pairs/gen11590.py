from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & F.col("customer_id").isin([15, 18, 33]))
    .groupBy(
        F.when(F.col("balance") >= 80000, "high").when(F.col("balance") >= 6000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .orderBy(F.col("stddev_balance").desc(), F.col("balance_bucket").desc())
    .limit(50)
)
