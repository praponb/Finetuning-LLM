from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("avg_quantity") < 62)
    .orderBy("last_last_restock", "store_id")
    .limit(20)
)
