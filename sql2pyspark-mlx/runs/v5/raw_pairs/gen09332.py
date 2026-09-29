from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("brand")
)
