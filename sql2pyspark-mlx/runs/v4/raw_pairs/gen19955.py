from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]) & (F.col("product_id") == 4))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.count("*").alias("num_inventory"),
        F.count("*").alias("total_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("total_count") >= 5)
    .orderBy(F.col("num_inventory").desc())
)
