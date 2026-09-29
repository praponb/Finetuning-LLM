from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.supplier_id").isin([11, 17, 26]))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("id").desc())
)
