from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([9, 15, 20, 25]) & (F.col("quantity") == 6)) | F.col("quantity").between(4, 6))
    .agg(F.count("quantity").alias("quantity_count"))
)
