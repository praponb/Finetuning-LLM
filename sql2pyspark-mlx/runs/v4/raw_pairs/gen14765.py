from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 8) & (F.col("unit_price") >= 110) & F.col("product_id").isin([1, 9, 21, 26]))
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("product_id").desc())
)
