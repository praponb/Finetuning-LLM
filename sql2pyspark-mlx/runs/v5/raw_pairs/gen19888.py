from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([4, 5, 8, 10]))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.brand"),
        F.col("s.rating"),
    )
    .orderBy(F.col("product_id").desc())
)
