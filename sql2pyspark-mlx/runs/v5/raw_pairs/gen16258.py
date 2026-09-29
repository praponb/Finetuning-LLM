from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("product_id").desc())
)
