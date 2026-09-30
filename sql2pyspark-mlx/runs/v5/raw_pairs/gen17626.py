from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 370) | (F.col("product_id") == 31))
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 1300)
)
