from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 17) & F.col("product_id").isNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)
