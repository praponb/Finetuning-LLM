from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("product_id").desc())
)
