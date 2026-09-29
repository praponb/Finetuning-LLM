from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") < "2023-05-01"))
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)
