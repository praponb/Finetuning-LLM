from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id").isin([1, 3, 4]))
    .select(F.col("p.price"), F.col("p.stock_qty"), F.col("i.product_id"))
)
