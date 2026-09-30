from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([7, 31, 32]) & F.col("order_id").isin([11, 14, 23]) & F.col("product_id").isin([5, 36]))
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 1000)
    .orderBy("product_id")
)
