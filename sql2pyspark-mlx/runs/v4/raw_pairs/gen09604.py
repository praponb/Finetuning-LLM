from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .filter(F.col("s.sqft") >= 6197)
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.cost"),
    )
    .orderBy("category", "sqft")
)
