from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.store_name").like("Ai%") & (F.col("ss.units") != 32))
    .select(
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("p.price"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("price").desc())
)
