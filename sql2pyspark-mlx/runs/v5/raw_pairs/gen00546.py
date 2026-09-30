from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 6]) & F.col("store_id").isin([2, 5]))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = product_id_summary.select("product_id", "last_last_restock")
