from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id").isin([12, 23]))
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_price") >= 54)
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.city"), F.col("t.stddev_price"))
    .orderBy(F.col("city").desc(), "id")
    .limit(1)
)
