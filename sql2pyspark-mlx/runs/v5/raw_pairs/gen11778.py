from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("ProductsBySupplier"), F.col("suppliers.id") == F.col("ProductsBySupplier.supplier_id"), "inner")
    .filter(F.col("ProductsBySupplier.max_cost") >= 620)
    .select(F.col("suppliers.city"), F.col("ProductsBySupplier.max_cost"))
)
