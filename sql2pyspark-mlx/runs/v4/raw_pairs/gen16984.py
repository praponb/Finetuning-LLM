from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("i.last_restock")).isin([1, 2, 3]))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("quantity").desc())
)
