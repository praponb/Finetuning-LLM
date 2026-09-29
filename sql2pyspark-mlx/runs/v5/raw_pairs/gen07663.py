from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 2, 4, 5]) & (F.length(F.col("s.store_name")) >= 7))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.when(F.col("p.stock_qty") >= 364, "High").when(F.col("p.stock_qty") >= 101, "Medium").otherwise("Low").alias("stock_qty_level"),
    )
    .orderBy(F.col("region").desc(), "opened_date")
)
