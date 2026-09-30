from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .groupBy(
        "account_type",
        "status",
        F.when(F.col("balance") >= 65000, "high").when(F.col("balance") >= 30000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
    .orderBy("balance_range", "account_type", "status", "balance_bucket")
    .limit(1)
)
