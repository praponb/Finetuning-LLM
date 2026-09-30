from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 35)
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.countDistinct("balance").alias("unique_customers"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
    )
)
