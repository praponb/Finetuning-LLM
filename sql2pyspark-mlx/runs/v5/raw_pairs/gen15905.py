from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("cost").alias("unique_brands"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.city"), F.col("t.unique_brands"))
    .orderBy("unique_brands")
)
