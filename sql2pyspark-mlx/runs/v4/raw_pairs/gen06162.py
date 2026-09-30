from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty").between(102, 258))
    .select(F.col("p.category"), F.col("i.product_id"))
)
