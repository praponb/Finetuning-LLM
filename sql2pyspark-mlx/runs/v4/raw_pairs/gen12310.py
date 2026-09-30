from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(230, 400))
    .groupBy("product_id", "order_id")
    .agg(F.count("*").alias("num_order_items"))
    .orderBy(F.col("order_id").desc())
)
