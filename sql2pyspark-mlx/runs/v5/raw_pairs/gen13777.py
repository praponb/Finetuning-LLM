from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .groupBy(
        F.when(F.col("balance") >= 77000, "high").when(F.col("balance") >= 27000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy("balance_bucket", "balance_count")
)
