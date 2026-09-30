from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]) & (F.col("product_id").isin([1, 3, 5, 7]) | (F.col("quantity") >= 89)))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 1220)
    .orderBy("last_restock_month")
)
