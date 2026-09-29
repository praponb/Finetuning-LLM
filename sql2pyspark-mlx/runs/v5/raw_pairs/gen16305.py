from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.product_id").isin([1, 4, 9]))
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy(F.col("inventory_count").desc(), "product_name", "id")
    .limit(1)
)
