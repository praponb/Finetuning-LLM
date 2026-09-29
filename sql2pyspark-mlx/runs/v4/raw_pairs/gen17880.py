from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.supplier_id").isNull() & ~F.col("suppliers.supplier_name").like("Eur%"))
    .select(
        F.col("products.price"),
        F.col("products.supplier_id"),
        F.col("suppliers.rating"),
        F.col("suppliers.country"),
        F.col("suppliers.city"),
    )
    .orderBy(F.col("supplier_id").desc())
)
