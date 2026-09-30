from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 30) | F.col("quantity").between(3, 7))
    .rollup("order_id", "product_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy("product_id", "order_id")
)
