from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.launch_date") > "2025-09-01") | (F.col("p.cost") > 550))
    .select(F.col("i.last_restock"), F.col("i.store_id"), F.col("p.stock_qty"))
    .orderBy(F.col("store_id").desc())
)
