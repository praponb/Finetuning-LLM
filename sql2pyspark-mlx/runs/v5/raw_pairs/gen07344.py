from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 7)
    .groupBy("order_id", "product_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 2)
)
