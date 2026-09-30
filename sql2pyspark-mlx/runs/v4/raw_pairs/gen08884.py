from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(72, 74))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") > 4)
    .orderBy(F.col("unique_products").desc())
)
