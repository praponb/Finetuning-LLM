from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy(F.col("last_last_restock").desc())
)
