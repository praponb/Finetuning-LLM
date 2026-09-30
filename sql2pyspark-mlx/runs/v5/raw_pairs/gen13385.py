from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("i.store_id") == 4) & (F.col("p.stock_qty") <= 173))
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("product_id").desc())
)
