from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 64000, "high").when(F.col("balance") >= 10000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
    .filter(F.col("pct_closed") < 28)
    .orderBy("pct_closed", F.col("balance_bucket").desc())
)
