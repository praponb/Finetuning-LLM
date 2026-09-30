from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 220)
    .drop("avg_quantity")
)
