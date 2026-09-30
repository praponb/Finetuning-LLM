from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") != "Singapore") & F.col("i.quantity").between(152, 212))
    .select(
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.category"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.round((F.col("i.quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)
