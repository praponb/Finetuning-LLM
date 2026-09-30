from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("method", ["promptpay", "cash", "paypal"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
