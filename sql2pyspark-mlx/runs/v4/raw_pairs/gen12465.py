from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "new"))
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.price"),
        F.col("suppliers.country"),
    )
    .orderBy(F.col("id").desc(), "country")
)
