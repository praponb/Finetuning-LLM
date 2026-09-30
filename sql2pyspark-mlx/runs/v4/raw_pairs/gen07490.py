from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("ss.revenue").between(1300, 1400))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy(F.col("product_id").desc())
)
