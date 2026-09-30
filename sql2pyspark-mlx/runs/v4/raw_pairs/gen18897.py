from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([1, 10, 21, 39]))
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)
