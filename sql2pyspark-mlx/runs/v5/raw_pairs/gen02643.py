from pyspark.sql import functions as F

valid_accounts = df_accounts.filter((F.col("status") == "FROZEN") & (F.col("status") == "FROZEN"))

agg = (
    valid_accounts
    .groupBy(
        F.when(F.col("balance") >= 58000, "high").when(F.col("balance") >= 22000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = agg.select("balance_bucket", "open_count", "first_opened_date")
