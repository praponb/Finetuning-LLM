from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price") >= 1130)
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("supplier_name").desc())
)
