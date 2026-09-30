from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("p.product_name"),
        F.add_months(F.col("i.last_restock"), 12).alias("last_restock_plus_months"),
    )
)
