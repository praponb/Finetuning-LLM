from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy("id", F.col("inventory_count").desc())
)
