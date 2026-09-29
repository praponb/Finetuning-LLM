from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("stock_qty").alias("unique_brands"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.rating"), F.col("pbs.unique_brands"))
    .orderBy("id", F.col("unique_brands").desc())
)
