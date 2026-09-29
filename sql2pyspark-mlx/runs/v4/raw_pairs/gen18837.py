from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3]))
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
)
