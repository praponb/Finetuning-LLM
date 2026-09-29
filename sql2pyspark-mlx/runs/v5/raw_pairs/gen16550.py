from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price").isNull())
    .groupBy("supplier_id")
    .agg(F.sum("price").alias("total_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_price") >= 4200)
    .select(F.col("s.id"), F.col("s.country"), F.col("t.total_price"))
    .orderBy(F.col("total_price").desc())
)
