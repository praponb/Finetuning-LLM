from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("method", ["card", "promptpay"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
