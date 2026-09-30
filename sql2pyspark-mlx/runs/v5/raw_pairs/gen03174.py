from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 35)
    .withColumn("clean_status", F.trim("status"))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
)
