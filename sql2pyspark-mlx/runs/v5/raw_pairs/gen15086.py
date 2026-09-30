from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["North", "West", "Central"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("id").desc())
)
