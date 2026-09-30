from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") >= 30)
    .orderBy(F.col("min_last_restock").desc(), "product_id")
    .limit(3)
)
