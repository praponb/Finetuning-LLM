from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy(F.col("unique_products").desc())
)
