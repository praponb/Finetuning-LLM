from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.store_id").isin([2, 3, 4]))
    .select(
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("p.supplier_id"),
    )
)
