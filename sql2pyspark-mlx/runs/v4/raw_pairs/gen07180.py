from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .subtract(df_orders.select(F.col("order_date").alias("event_date")))
)
