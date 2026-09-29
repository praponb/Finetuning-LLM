from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .pivot("method", ["promptpay", "paypal"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
