from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy(
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 9000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("balance").alias("min_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
)
