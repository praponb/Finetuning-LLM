from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
    .filter(F.col("min_unit_price") > 310)
    .select("order_id", "product_id", "min_unit_price")
)
