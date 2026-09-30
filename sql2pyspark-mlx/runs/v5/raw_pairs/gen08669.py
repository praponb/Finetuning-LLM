from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.length(F.col("s.store_name")).alias("store_name_length"),
    )
    .orderBy("city", "id")
    .limit(10)
)
