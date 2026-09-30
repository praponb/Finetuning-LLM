from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("channel", ["store", "partner"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
