from pyspark.sql import functions as F

inventory_by_product = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-05-14")
    .groupBy("product_id")
    .agg(F.max("last_restock").alias("last_last_restock"))
)

result = (
    df_products.alias("p")
    .join(inventory_by_product.alias("I"), F.col("p.id") == F.col("I.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("I.last_last_restock"))
)
