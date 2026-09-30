from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.product_id") == 6)
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("p.supplier_id"),
    )
)
