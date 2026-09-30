from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.substring(F.col("p.product_name"), 1, 2).alias("product_name_prefix"),
    )
    .orderBy(F.col("product_id").desc())
)
