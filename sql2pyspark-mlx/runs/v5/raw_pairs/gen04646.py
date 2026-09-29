from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5]))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 180)
    .orderBy("store_id")
    .limit(10)
)
