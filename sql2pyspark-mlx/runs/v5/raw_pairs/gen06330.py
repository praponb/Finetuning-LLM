from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.quantity").between(186, 211) | F.col("i.store_id").isin([2, 4, 6]))
    .select(F.col("i.product_id"), F.col("p.brand"))
)
