from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("s.country") != "SG") | F.col("s.supplier_name").like("%Cit%"))
    .select(
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.rating"),
    )
)
