from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(F.count("*").alias("num_order_items"))
    .orderBy(F.col("product_id").desc())
)
