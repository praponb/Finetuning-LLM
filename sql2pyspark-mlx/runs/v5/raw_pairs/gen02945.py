from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~(F.col("p.product_name").like("%Hub%") | (F.col("p.cost") <= 410)))
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy(F.col("supplier_name").desc(), "supplier_id")
)
