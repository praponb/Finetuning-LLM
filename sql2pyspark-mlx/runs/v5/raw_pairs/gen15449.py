from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "status")
    .pivot("method", ["card", "promptpay"])
    .agg(F.sum("amount").alias("total"))
)
