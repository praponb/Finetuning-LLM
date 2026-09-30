from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.category").isin(["Grocery", "Home", "Electronics"]) & (F.col("p.price") >= 680))
    .select(F.col("p.brand"), F.col("s.supplier_name"))
    .orderBy(F.col("supplier_name").desc())
)
