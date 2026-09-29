from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([7, 16, 38, 39]))
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)
