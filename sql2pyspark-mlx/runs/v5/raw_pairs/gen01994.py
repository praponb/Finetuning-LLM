from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter(F.col("i.store_id").isin([2, 6]))
    .select(F.col("i.product_id"), F.col("i.store_id"), F.col("p.cost"))
)
