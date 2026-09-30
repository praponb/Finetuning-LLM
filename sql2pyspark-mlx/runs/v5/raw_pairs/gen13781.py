from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(F.col("p.cost"), F.col("i.store_id"))
    .orderBy("store_id", "cost")
)
