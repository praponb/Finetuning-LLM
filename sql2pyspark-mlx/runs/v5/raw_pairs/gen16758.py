from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 85000, "high").when(F.col("balance") >= 15000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
        "status",
    )
    .agg(F.max("balance").alias("max_balance"))
)
