from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
)
