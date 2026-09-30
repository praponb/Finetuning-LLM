from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 12, 13]))
    .groupBy("status", "account_type")
    .agg(F.min("balance").alias("min_balance"))
)
