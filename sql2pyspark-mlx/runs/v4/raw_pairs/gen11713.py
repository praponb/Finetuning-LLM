from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.supplier_id") == 35) | (F.col("p.price") < 620))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
    )
)
