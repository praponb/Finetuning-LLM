from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
    .orderBy(F.col("line_total").desc(), "id")
    .limit(50)
)
