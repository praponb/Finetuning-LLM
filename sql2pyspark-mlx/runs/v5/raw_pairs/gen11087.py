from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 7) | F.col("order_id").isin([22, 28, 29, 36]))
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") > 45)
    .orderBy(F.col("product_id").desc())
)
