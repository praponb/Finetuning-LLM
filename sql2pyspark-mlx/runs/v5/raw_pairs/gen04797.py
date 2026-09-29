from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .withColumn("account_type_status_key", F.concat_ws("-", "account_type", "status"))
)

result = (
    cleaned
    .groupBy("account_type", "customer_id")
    .pivot("status", ["OPEN", "FROZEN"])
    .agg(F.avg("balance").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
