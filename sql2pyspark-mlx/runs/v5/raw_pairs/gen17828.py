from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(72, 210))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 160)
)
