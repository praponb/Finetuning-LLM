from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "COMPLETED") & (F.col("shipping_fee") <= 13))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
    .orderBy(F.col("order_date").desc())
)
