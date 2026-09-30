from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        F.max("opened_date").alias("latest_opened_date"),
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .filter(F.col("credit_count") > 78)
    .orderBy("credit_count", "status")
)
