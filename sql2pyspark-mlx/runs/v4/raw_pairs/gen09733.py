from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 8]) & F.col("store_id").isin([1, 3, 5]) & F.col("store_id").isin([1, 4, 6]))
    .withColumn("quantity_whole", F.floor("quantity"))
)
