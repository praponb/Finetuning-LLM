from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 5.9)
    .select("order_id", "min_quantity")
    .orderBy(F.col("min_quantity").desc(), F.col("order_id").desc())
    .limit(10)
)
