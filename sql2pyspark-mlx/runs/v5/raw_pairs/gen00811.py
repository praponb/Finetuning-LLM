from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-01-01", "2025-07-01") | F.col("last_restock").between("2023-12-26", "2025-02-01"))
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .orderBy("store_id")
)
