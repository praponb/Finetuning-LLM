from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNull() & (F.col("quantity") <= 64))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.min("last_restock").alias("min_last_restock"))
)
