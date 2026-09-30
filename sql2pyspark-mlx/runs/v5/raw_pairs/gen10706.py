from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock") < "2023-05-01")
    .select(F.col("i.store_id"), F.col("p.product_name"), F.col("s.city"))
    .orderBy("city")
)
