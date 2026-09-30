from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") <= 2.4)
    .select(
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.rating"),
        F.lower(F.col("p.product_name")).alias("product_name_lower"),
    )
    .orderBy("category")
)
