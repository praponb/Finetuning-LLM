from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 6]))
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy("product_id")
)
