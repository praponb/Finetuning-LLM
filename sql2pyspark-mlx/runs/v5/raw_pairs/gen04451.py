from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.year("opened_date") >= 2023) & (~F.col("account_type").isin(["CREDIT", "LOAN"]) | F.col("customer_id").isNotNull()))
)

balance_bucket_summary = (
    recent_accounts
    .groupBy(
        F.when(F.col("balance") >= 73000, "high").when(F.col("balance") >= 6000, "mid").otherwise("low").alias("balance_bucket"),
        "status",
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("cnt"),
    )
)

result = (
    balance_bucket_summary
    .filter(F.col("cnt") >= 8)
    .select("balance_bucket", "status", "avg_balance", "cnt")
    .orderBy("avg_balance", "balance_bucket", "status")
    .limit(10)
)
