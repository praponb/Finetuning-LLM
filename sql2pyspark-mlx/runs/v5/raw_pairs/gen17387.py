from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) | F.col("store_id").isin([3, 4, 5]))
    .rollup(F.year("last_restock").alias("last_restock_year"), "store_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .orderBy(F.col("last_restock_year").desc())
)
