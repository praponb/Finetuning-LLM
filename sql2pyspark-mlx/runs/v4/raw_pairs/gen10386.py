from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "status")
    .pivot("channel", ["web", "mobile", "partner"])
    .agg(F.sum("amount").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
