from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"))
    .intersect(df_store_sales.select(F.col("revenue").alias("value")))
)
