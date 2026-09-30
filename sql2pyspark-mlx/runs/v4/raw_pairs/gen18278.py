from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 26)
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)
