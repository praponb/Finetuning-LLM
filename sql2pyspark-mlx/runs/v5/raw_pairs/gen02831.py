from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") == 395)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
    .orderBy("stock_qty")
)
