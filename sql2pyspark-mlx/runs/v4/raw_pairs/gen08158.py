from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.year(F.col("s.opened_date")) >= 2024) & (F.col("i.last_restock") >= "2023-12-28"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("stock_qty").desc())
)
