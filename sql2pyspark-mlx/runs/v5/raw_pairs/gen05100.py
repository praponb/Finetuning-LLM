from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.cost") >= 230)
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.lower(F.col("s.supplier_name")).alias("supplier_name_lower"),
    )
    .orderBy(F.col("rating").desc())
)
