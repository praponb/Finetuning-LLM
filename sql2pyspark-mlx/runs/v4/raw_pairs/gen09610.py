from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("p.category") == "Books")
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy(F.col("inventory_count").desc())
)
