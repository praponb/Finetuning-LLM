from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy(F.col("last_restock_quarter").desc())
)
