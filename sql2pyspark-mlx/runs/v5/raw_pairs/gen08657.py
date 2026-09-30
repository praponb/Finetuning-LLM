from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 29000)
    .groupBy(
        "status",
        F.when(F.col("balance") >= 60000, "high").when(F.col("balance") >= 21000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .orderBy(F.col("avg_balance").desc(), F.col("status").desc())
)
