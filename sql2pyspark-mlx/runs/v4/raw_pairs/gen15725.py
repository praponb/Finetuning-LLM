from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2025-09-01") | (F.col("quantity") > 154))
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("quantity_range") > 32)
    .orderBy(F.col("quantity_range").desc())
)
