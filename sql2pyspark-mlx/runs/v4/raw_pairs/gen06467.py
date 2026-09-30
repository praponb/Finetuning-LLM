from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("channel", ["partner", "store", "web"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
