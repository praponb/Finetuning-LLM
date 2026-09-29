from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .pivot("status", ["FROZEN", "CLOSED", "OPEN"])
    .agg(F.max("balance").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
