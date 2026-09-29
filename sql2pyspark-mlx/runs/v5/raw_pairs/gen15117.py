from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") == "Singapore") & (F.col("p.stock_qty") <= 377))
    .select(F.col("s.rating"), F.col("p.brand"))
)
