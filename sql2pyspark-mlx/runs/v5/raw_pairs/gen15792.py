from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("ss.revenue").between(1700, 3000) & F.month(F.col("s.opened_date")).isin([4, 7, 9]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("brand").desc(), "supplier_id")
)
