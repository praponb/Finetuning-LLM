from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") >= 221) & F.col("last_restock").between("2025-02-01", "2025-10-01") & F.col("store_id").isin([3, 4]))
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .orderBy("store_id", "first_last_restock")
)
