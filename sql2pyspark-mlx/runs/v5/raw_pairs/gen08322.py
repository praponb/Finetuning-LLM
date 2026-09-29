from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 2, 3]) & (F.col("customer_id") == 6))
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)
