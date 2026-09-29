from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "merchant")
    .pivot("txn_type", ["FEE", "CREDIT"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
