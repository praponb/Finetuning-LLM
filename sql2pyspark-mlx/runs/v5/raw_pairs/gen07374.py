from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
)
