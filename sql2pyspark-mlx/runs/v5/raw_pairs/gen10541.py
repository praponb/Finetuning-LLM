from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "bestseller") & F.col("p.product_name").like("W%"))
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
    )
    .orderBy("rating")
)
