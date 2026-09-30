from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 180)
    .drop("max_quantity")
    .orderBy(F.col("avg_quantity").desc(), F.col("store_id").desc())
)
