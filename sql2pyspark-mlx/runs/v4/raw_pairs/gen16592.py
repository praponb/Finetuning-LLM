from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.brand") != "Acme")
    .select(
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("store_name").desc())
)
