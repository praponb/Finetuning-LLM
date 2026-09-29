from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        F.count("*").alias("total_count"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("total_count", "status")
    .limit(50)
)
