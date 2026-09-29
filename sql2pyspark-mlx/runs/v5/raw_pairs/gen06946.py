from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.col("unit_price").alias("value"))
    .unionByName(df_store_sales.select(F.col("revenue").alias("value")))
    .distinct()
    .orderBy("value")
)
