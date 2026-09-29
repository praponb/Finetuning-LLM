from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .groupBy(
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 14000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("pct_open") >= 78)
    .orderBy(F.col("pct_open").desc(), "balance_bucket")
)
