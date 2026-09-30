from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 12, 20, 32]) & F.col("balance").isNotNull() & F.col("customer_id").isin([15, 27, 40]))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.max("balance").alias("max_balance"))
)
