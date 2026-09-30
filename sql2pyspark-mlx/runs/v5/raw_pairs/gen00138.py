from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 14]))
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
    .withColumn("line_total_2", (F.col("unit_price") - F.col("quantity")))
)
