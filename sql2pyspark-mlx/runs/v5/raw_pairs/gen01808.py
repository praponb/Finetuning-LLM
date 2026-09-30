from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
    .orderBy("product_id")
)
