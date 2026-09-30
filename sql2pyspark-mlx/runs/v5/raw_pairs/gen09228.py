from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([1, 4, 6]) & (F.col("store_id") == 6) & F.col("store_id").isin([1, 4, 6])))
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(F.min("last_restock").alias("first_last_restock"))
)
