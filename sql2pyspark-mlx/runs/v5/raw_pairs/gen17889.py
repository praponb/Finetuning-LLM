from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("quantity") != 195) & (F.col("last_restock") <= "2025-06-01"))
)

agg = (
    recent_inventory
    .groupBy("product_id", "store_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    agg
    .filter(F.col("unique_stores") < 6)
    .select("product_id", "store_id", "unique_stores")
)
