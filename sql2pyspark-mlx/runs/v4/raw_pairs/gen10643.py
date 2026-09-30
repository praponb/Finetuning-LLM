from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .filter(F.col("ss.sale_date").between("2024-01-01", "2024-08-17"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("s.city"),
    )
)
