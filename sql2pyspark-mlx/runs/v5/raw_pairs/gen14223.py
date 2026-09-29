from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("p.product_name")).like("%ois%") | F.col("s.sqft").between(6475, 12329))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("s.city"),
    )
    .orderBy(F.col("stock_qty").desc())
)
