from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 68000, "high").when(F.col("balance") >= 24000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("pct_open") < 69)
)
