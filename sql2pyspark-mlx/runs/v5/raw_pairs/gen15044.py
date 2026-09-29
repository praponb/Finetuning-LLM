from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("brand") == "Globex")
    .groupBy("supplier_id")
    .agg(F.avg("price").alias("avg_price"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("pbs.avg_price"))
)
