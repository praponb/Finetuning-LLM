from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.store_id").isin([1, 3]))
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
    .orderBy("min_last_restock", "launch_year")
    .limit(3)
)
