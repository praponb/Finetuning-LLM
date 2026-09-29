from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") < 2)
    .orderBy(F.col("last_restock_quarter").desc())
)
