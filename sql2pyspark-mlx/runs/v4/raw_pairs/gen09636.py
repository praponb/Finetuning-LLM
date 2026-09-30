from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .agg(F.avg("quantity").alias("avg_quantity"))
)
