from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 69000, "high").when(F.col("balance") >= 14000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .filter(F.col("closed_count") < 51)
    .select("account_type", "balance_bucket", "closed_count")
    .orderBy("balance_bucket", "account_type")
    .limit(10)
)
