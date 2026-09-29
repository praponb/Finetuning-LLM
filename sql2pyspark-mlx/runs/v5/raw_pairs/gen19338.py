from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("quantity").between(4, 6))

quantity_bucket_summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
)

result = quantity_bucket_summary
