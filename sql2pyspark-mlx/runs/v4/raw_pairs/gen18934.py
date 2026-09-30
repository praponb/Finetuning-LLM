from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.last_restock") >= "2024-11-01")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.sqft"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
)
