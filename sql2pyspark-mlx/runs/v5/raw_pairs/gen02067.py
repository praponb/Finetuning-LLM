from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("brand").desc(), F.col("store_id").desc())
)
