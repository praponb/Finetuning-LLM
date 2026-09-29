from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.upper(F.col("s.store_name")).like("%IVE%"))
    .select(
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("s.city"),
    )
)
