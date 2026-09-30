from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 3)
    .groupBy("order_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 7.1)
    .select("order_id", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc(), F.col("order_id").desc())
)
