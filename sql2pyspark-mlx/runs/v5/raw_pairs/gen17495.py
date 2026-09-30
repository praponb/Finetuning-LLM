from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter((F.col("p.stock_qty") < 132) | F.col("s.store_name").like("%Old%"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.col("p.category"),
    )
)
