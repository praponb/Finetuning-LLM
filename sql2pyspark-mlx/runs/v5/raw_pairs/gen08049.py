from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 3) & (F.col("quantity") != 3))
    .groupBy("order_id")
    .agg(F.count("*").alias("order_item_count"))
    .orderBy("order_item_count", "order_id")
    .limit(5)
)
