from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2024-09-19") & (F.col("last_restock") > "2025-01-26"))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 210)
    .orderBy("product_id")
)
