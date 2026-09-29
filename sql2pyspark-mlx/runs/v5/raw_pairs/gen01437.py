from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.col("customer_id").isNotNull())

status_summary = (
    filtered_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 79000, "high").when(F.col("balance") >= 26000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)

result = (
    status_summary
    .filter(F.col("frozen_count") >= 48)
    .select("status", "balance_bucket", "frozen_count")
    .orderBy(F.col("frozen_count").desc(), F.col("status").desc())
)
