from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "order_id")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.avg("amount").alias("value_sum"))
)
