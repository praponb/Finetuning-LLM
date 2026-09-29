from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 9)
    .orderBy(F.col("order_id").desc(), "row_count")
    .limit(5)
)
