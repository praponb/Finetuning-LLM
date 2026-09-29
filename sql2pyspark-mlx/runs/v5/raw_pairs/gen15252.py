from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.sqft"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.trim(F.col("s.region")).alias("clean_region"),
    )
    .orderBy("category")
)
