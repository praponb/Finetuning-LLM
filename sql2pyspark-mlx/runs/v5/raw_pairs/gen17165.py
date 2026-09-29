from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("i.product_id") == 1) & (F.col("i.store_id") == 5))
    .select(F.col("p.stock_qty"), F.col("p.category"), F.col("i.product_id"))
)
