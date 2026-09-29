from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .filter(F.col("s.region").isin(["West", "East", "Central"]) & F.col("p.price").between(550, 570))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("s.city"),
    )
)
