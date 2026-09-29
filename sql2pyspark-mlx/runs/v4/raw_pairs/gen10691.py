from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.count("balance").alias("balance_count"))
    .filter(F.col("balance_count") >= 4)
    .select("customer_id", "balance_count")
)
