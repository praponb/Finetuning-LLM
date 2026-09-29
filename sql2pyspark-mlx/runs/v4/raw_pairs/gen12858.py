from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") > "2024-04-01").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .filter(F.col("p.product_name").isNotNull())
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.category"))
)
