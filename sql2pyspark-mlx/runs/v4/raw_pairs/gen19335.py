from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 8) & (F.col("order_id") == 38))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy("avg_quantity")
)
