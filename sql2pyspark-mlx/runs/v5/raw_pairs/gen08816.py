from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.max("order_date").alias("latest_order_date"))
    .orderBy("channel")
)
