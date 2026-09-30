from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 119) & F.col("product_id").isin([2, 5]) & F.col("store_id").isin([4, 5]))
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("min_quantity") >= 150)
    .orderBy("min_quantity", F.col("store_id").desc(), "last_restock_month")
    .limit(10)
)
