from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("price").between(630, 1130))
    .groupBy("supplier_id")
    .agg(F.max("price").alias("max_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.rating"), F.col("P.max_price"))
    .orderBy(F.col("rating").desc(), "id")
)
